import datetime
import logging
import os
import queue
import random
import threading
import time

from apscheduler.schedulers.background import BackgroundScheduler

from database import SessionLocal
from models import MonitorTask, ProductItem, SystemConfig
import scheduler_notifications as _notifications
from scheduler_health import (
    handle_empty_scrape_result as _handle_empty_scrape_result_impl,
    handle_successful_scrape as _handle_successful_scrape_impl,
)
from scheduler_retention import (
    prune_removed_products_history as _prune_removed_products_history_impl,
    resolve_product_retention_days as _resolve_product_retention_days_impl,
)
from utils import get_resource_path, probe_http_text, response_looks_blocked
try:
    from scraper import fetch_products_for_task, ScrapeCancelled, ScrapeIncomplete, ScrapeTransientError
except ModuleNotFoundError:
    # Fallback for packaged builds if module resolution fails.
    import importlib.util
    import sys as _sys

    _scraper_path = get_resource_path("scraper.py")
    if not os.path.exists(_scraper_path):
        raise
    _spec = importlib.util.spec_from_file_location("scraper", _scraper_path)
    if _spec is None or _spec.loader is None:
        raise
    _mod = importlib.util.module_from_spec(_spec)
    _sys.modules["scraper"] = _mod
    _spec.loader.exec_module(_mod)
    fetch_products_for_task = _mod.fetch_products_for_task
    ScrapeCancelled = _mod.ScrapeCancelled
    ScrapeIncomplete = _mod.ScrapeIncomplete
    ScrapeTransientError = _mod.ScrapeTransientError
from security import validate_monitor_target_url

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()

_task_state_lock = threading.Lock()
_inflight_task_ids: set[int] = set()
_running_task_id: int | None = None  # The single task currently being executed.
_queued_task_ids: list[int] = []     # Ordered list of task IDs waiting to run.

# Global serial execution queue — only one task runs at a time.
_execution_queue: queue.Queue[int | None] = queue.Queue()
_worker_thread: threading.Thread | None = None
_scheduler_init_lock = threading.Lock()
_scheduler_shutdown_lock = threading.Lock()

_force_stop_lock = threading.Lock()

# Network failure retry queue
_retry_queue_lock = threading.Lock()
_network_retry_queue: list[int] = []  # Task IDs pending retry due to network issues
_last_network_check: datetime.datetime | None = None
_network_healthy: bool = True
_network_issue_active: bool = False
_network_issue_event_id: int = 0
_last_network_issue_at: datetime.datetime | None = None
_last_network_issue_message: str = ""
_network_recovery_event_id: int = 0
_last_network_recovery_at: datetime.datetime | None = None
_last_network_recovery_message: str = ""

EMPTY_ALERT_THRESHOLD = 3  # Send health alert after this many consecutive 0-product scrapes.
DEFAULT_PRODUCT_RETENTION_DAYS = 90
CLEANUP_JOB_ID = "prune_removed_products"
NETWORK_CHECK_JOB_ID = "network_check_retry"
_TASK_RETRY_ATTEMPTS = 2       # How many extra retries if scrape returns nothing.
_TASK_RETRY_DELAY = (10, 20)   # Random delay (seconds) between retries.
_INTER_TASK_DELAY = (3, 8)     # Random delay (seconds) between consecutive tasks.
_SCRAPE_TIMEOUT = 600          # Max seconds for a single scrape attempt.
_SMTP_RETRY_ATTEMPTS = 2      # Extra retries for email sending.
_NETWORK_CHECK_INTERVAL_MINUTES = 30  # Check network every 30 minutes when tasks are pending retry
_NETWORK_ALERT_POPUP_COOLDOWN_SECONDS = max(
    60, int(os.getenv("MONITOR_WEB_NETWORK_ALERT_COOLDOWN_SECONDS", "900") or "900")
)

_clean_subject_text = _notifications.clean_subject_text
_parse_recipients = _notifications.parse_recipients
_safe_link_for_html = _notifications.safe_link_for_html
_load_smtp_context = _notifications.load_smtp_context
_render_product_table = _notifications.render_product_table


def get_inflight_task_ids() -> set[int]:
    with _task_state_lock:
        return set(_inflight_task_ids)


def get_queue_snapshot() -> tuple[int | None, list[int]]:
    """Return (currently_running_task_id, [waiting_task_ids])."""
    with _task_state_lock:
        logger.debug("Queue snapshot: running=%s, waiting=%s", _running_task_id, _queued_task_ids)
        return _running_task_id, list(_queued_task_ids)


def _acquire_task_slot(task_id: int) -> bool:
    with _task_state_lock:
        if task_id in _inflight_task_ids:
            return False
        _inflight_task_ids.add(task_id)
        return True


def _release_task_slot(task_id: int) -> None:
    with _task_state_lock:
        _inflight_task_ids.discard(task_id)


# ---------------------------------------------------------------------------
# Network Retry Queue Management
# ---------------------------------------------------------------------------

def _mark_network_issue(message: str) -> None:
    """Mark a visible network/access issue for frontend reminder popup."""
    global _network_issue_active, _network_issue_event_id, _last_network_issue_at, _last_network_issue_message

    issue_message = (message or "").strip() or (
        "检测到抓取网络或访问异常，建议切换网络、重连代理/VPN 后再观察自动重试结果。"
    )
    if not _network_issue_active:
        _network_issue_event_id += 1
    _network_issue_active = True
    _last_network_issue_at = datetime.datetime.now(datetime.timezone.utc)
    _last_network_issue_message = issue_message


def _mark_network_recovery(message: str) -> None:
    global _network_recovery_event_id, _last_network_recovery_at, _last_network_recovery_message
    _network_recovery_event_id += 1
    _last_network_recovery_at = datetime.datetime.now(datetime.timezone.utc)
    _last_network_recovery_message = (message or "").strip() or "网络访问已恢复，系统会继续自动重试之前失败的抓取任务。"


def _clear_network_issue(recovered: bool = False, message: str = "") -> None:
    global _network_issue_active
    was_active = _network_issue_active
    _network_issue_active = False
    if recovered and was_active:
        _mark_network_recovery(message)


def add_to_retry_queue(task_id: int) -> None:
    """Add a task to the network retry queue."""
    with _retry_queue_lock:
        if task_id not in _network_retry_queue:
            _network_retry_queue.append(task_id)
            _mark_network_issue(
                "检测到抓取网络或访问异常，部分任务已暂停判定并加入自动重试队列。"
            )
            logger.warning(
                "[RETRY] Task %s added to network retry queue. Queue size: %d",
                task_id, len(_network_retry_queue)
            )
            # Ensure network check job is scheduled
            _ensure_network_check_scheduled()


def get_retry_queue_snapshot() -> list[int]:
    """Get a copy of the current retry queue."""
    with _retry_queue_lock:
        return list(_network_retry_queue)


def _ensure_network_check_scheduled() -> None:
    """Ensure the network check job is scheduled when there are pending retries."""
    try:
        existing = scheduler.get_job(NETWORK_CHECK_JOB_ID)
        if not existing:
            scheduler.add_job(
                _network_check_and_retry,
                "interval",
                minutes=_NETWORK_CHECK_INTERVAL_MINUTES,
                id=NETWORK_CHECK_JOB_ID,
                replace_existing=True,
                next_run_time=datetime.datetime.now() + datetime.timedelta(minutes=_NETWORK_CHECK_INTERVAL_MINUTES),
            )
            logger.info(
                "[RETRY] Scheduled network check job every %d minutes.",
                _NETWORK_CHECK_INTERVAL_MINUTES
            )
    except Exception as e:
        logger.error("[RETRY] Failed to schedule network check job: %s", e)


def _check_network_health() -> bool:
    """Check if Amazon is accessible. Returns True if healthy."""
    test_urls = [
        "https://www.amazon.com",
        "https://www.amazon.com/robots.txt",
    ]
    
    for url in test_urls:
        probe = probe_http_text(url, timeout=15, max_bytes=2048)
        if probe.error_kind:
            logger.warning("[RETRY] Network check failed for %s: %s", url, probe.error_message)
            continue
        if probe.status_code == 200 and not response_looks_blocked(probe.final_url or url, probe.body_text):
            return True
    
    return False


def _network_check_and_retry() -> None:
    """Periodic job to check network and retry failed tasks."""
    global _network_healthy, _last_network_check
    
    with _retry_queue_lock:
        pending_count = len(_network_retry_queue)
        if pending_count == 0:
            _clear_network_issue()
            # No pending tasks, remove the job
            try:
                scheduler.remove_job(NETWORK_CHECK_JOB_ID)
                logger.info("[RETRY] No pending tasks, removed network check job.")
            except Exception as exc:
                logger.debug("[RETRY] Failed to remove network check job: %s", exc)
            return
    
    logger.info("[RETRY] Checking network health... (%d tasks pending retry)", pending_count)
    _last_network_check = datetime.datetime.now(datetime.timezone.utc)
    
    is_healthy = _check_network_health()
    _network_healthy = is_healthy
    
    if not is_healthy:
        with _retry_queue_lock:
            _mark_network_issue(
                "检测到当前网络无法稳定访问 Amazon，建议切换网络或重新连接代理/VPN。"
            )
        logger.warning(
            "[RETRY] Network still unhealthy. Will retry in %d minutes.",
            _NETWORK_CHECK_INTERVAL_MINUTES
        )
        return
    
    logger.info("[RETRY] Network healthy! Requeuing %d failed tasks...", pending_count)
    
    # Move all pending tasks to execution queue
    with _retry_queue_lock:
        tasks_to_retry = list(_network_retry_queue)
        _network_retry_queue.clear()
        _clear_network_issue(
            recovered=True,
            message="网络访问已恢复，系统正在自动重试之前受影响的抓取任务。",
        )
    
    for task_id in tasks_to_retry:
        queue_monitor_task(task_id)
        logger.info("[RETRY] Task %s requeued for execution.", task_id)


def get_network_retry_status() -> dict:
    """Get status of network retry system for display."""
    with _retry_queue_lock:
        return {
            "pending_count": len(_network_retry_queue),
            "pending_tasks": list(_network_retry_queue),
            "network_healthy": _network_healthy,
            "last_check": _last_network_check.isoformat() if _last_network_check else None,
            "check_interval_minutes": _NETWORK_CHECK_INTERVAL_MINUTES,
            "alert_active": _network_issue_active and bool(_network_retry_queue),
            "alert_event_id": _network_issue_event_id,
            "last_issue_at": _last_network_issue_at.isoformat() if _last_network_issue_at else None,
            "last_issue_message": _last_network_issue_message,
            "recovery_event_id": _network_recovery_event_id,
            "last_recovery_at": _last_network_recovery_at.isoformat() if _last_network_recovery_at else None,
            "last_recovery_message": _last_network_recovery_message,
            "popup_cooldown_seconds": _NETWORK_ALERT_POPUP_COOLDOWN_SECONDS,
        }


def _smtp_send(config, smtp_password, msg, recipients) -> None:
    _notifications.smtp_send(
        config,
        smtp_password,
        msg,
        recipients,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
    )


def send_email(db, task, new_products: list, removed_products: list | None = None):
    _notifications.send_email(
        db,
        task,
        new_products,
        removed_products,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
    )


def _send_health_alert(db, task, consecutive_count: int) -> None:
    _notifications.send_health_alert(
        db,
        task,
        consecutive_count,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
    )


def _send_recovery_notification(db, task, prev_count: int) -> None:
    _notifications.send_recovery_notification(
        db,
        task,
        prev_count,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
    )


def _resolve_product_retention_days(db) -> int:
    return _resolve_product_retention_days_impl(
        db,
        SystemConfig,
        DEFAULT_PRODUCT_RETENTION_DAYS,
    )


def prune_removed_products_history() -> None:
    _prune_removed_products_history_impl(
        SessionLocal,
        ProductItem,
        SystemConfig,
        DEFAULT_PRODUCT_RETENTION_DAYS,
    )


def execute_monitor_task(task_id: int):
    """Called by APScheduler interval jobs. Enqueues the task for serial execution."""
    if not _acquire_task_slot(task_id):
        logger.warning("Task %s is already queued or running, skipping interval trigger.", task_id)
        return
    with _task_state_lock:
        _queued_task_ids.append(task_id)
    _execution_queue.put(task_id)
    logger.info("Task %s enqueued by scheduled trigger.", task_id)


def _fetch_with_retry(db, task_id: int, task_name: str):
    """Attempt scrape up to 1 + _TASK_RETRY_ATTEMPTS times for transient failures.
    
    If a transient scrape error is raised, adds task to retry queue.
    Returns (current_products, new_products, removed_products) or raises the transient error.
    """
    from scraper import ScrapeCancelled, ScrapeIncomplete, ScrapeTransientError
    
    for attempt in range(1 + _TASK_RETRY_ATTEMPTS):
        try:
            current, new_prods, removed_prods = fetch_products_for_task(db, task_id)
            # Success - return results
            return current, new_prods, removed_prods
            
        except ScrapeCancelled:
            logger.warning("Task %s was cancelled, not retrying.", task_name)
            return [], [], []
            
        except (ScrapeIncomplete, ScrapeTransientError) as e:
            logger.warning(
                "[RETRY] Task %s transient scrape failure (attempt %d/%d): %s",
                task_name, attempt + 1, 1 + _TASK_RETRY_ATTEMPTS, e
            )
            # On last attempt, add to retry queue and re-raise
            if attempt >= _TASK_RETRY_ATTEMPTS:
                logger.warning(
                    "[RETRY] Task %s failed all %d attempts - adding to network retry queue.",
                    task_name, 1 + _TASK_RETRY_ATTEMPTS
                )
                add_to_retry_queue(task_id)
                raise  # Re-raise to signal failure
            
            # Check if cancelled before retrying
            from scraper import clear_cancel_scrape, is_cancel_requested
            if is_cancel_requested():
                logger.warning("Task %s retry cancelled by user.", task_name)
                clear_cancel_scrape()
                return [], [], []
            
            # Wait before retry
            delay = random.uniform(*_TASK_RETRY_DELAY)
            logger.warning(
                "Task %s transient failure, retrying in %.0fs...",
                task_name, delay
            )
            time.sleep(delay)
            
        except Exception as e:
            logger.error("Task %s unexpected error: %s", task_name, e)
            if attempt >= _TASK_RETRY_ATTEMPTS:
                raise
            time.sleep(random.uniform(*_TASK_RETRY_DELAY))
    
    # Should not reach here, but return empty if we do
    return [], [], []


def _execute_monitor_task_locked(task_id: int):
    logger.info("Executing scheduled task %s", task_id)
    db = SessionLocal()
    try:
        task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
        if not task or not task.is_active:
            return

        if not _validate_task_url(task_id, task.url):
            return

        current, new_prods, removed_prods = _fetch_with_retry(db, task_id, task.name)
        is_first_successful_run = task.last_run_at is None and bool(current)

        if not current:
            _handle_empty_scrape_result(db, task)
            return

        _handle_successful_scrape(db, task, current, new_prods, removed_prods, is_first_successful_run)

    except ScrapeCancelled:
        logger.warning("Task %s cancelled by user.", task_id)
        return
    except Exception as exc:
        # Check if it's a transient scrape failure - already handled in _fetch_with_retry
        from scraper import ScrapeIncomplete, ScrapeTransientError
        if isinstance(exc, (ScrapeIncomplete, ScrapeTransientError)):
            logger.warning(
                "[RETRY] Task %s hit a transient scrape failure - will retry later.",
                task_id
            )
            # Don't update any state - task is in retry queue
            return
        logger.error("Error executing task %s: %s", task_id, exc)
    finally:
        db.close()
        # When task execution is called outside the queue worker (e.g. tests/manual),
        # flush digest immediately so notifications are not stuck in memory.
        if not _is_queue_worker_thread():
            _flush_digest()


def _validate_task_url(task_id: int, url: str) -> bool:
    """Validate task URL against security policy."""
    try:
        validate_monitor_target_url(url)
        return True
    except ValueError as exc:
        logger.error("Task %s blocked by URL security policy: %s", task_id, exc)
        return False


def _handle_empty_scrape_result(db, task) -> None:
    _handle_empty_scrape_result_impl(
        db,
        task,
        empty_alert_threshold=EMPTY_ALERT_THRESHOLD,
        send_health_alert=_send_health_alert,
    )


def _handle_successful_scrape(db, task, current, new_prods, removed_prods, is_first_successful_run: bool) -> None:
    _handle_successful_scrape_impl(
        db,
        task,
        current,
        new_prods,
        removed_prods,
        is_first_successful_run,
        empty_alert_threshold=EMPTY_ALERT_THRESHOLD,
        send_recovery_notification=_send_recovery_notification,
        queue_digest_entry=_queue_digest_entry,
        is_queue_worker_thread=_is_queue_worker_thread,
        send_email=send_email,
    )


def _queue_digest_entry(
    task_name: str,
    new_products: list,
    removed_products: list,
    is_baseline: bool = False,
    baseline_count: int = 0,
) -> None:
    _notifications.queue_digest_entry(
        task_name,
        new_products,
        removed_products,
        is_baseline=is_baseline,
        baseline_count=baseline_count,
    )


def _flush_digest() -> None:
    _notifications.flush_digest(
        SessionLocal,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
    )


def _send_consolidated_email(db, entries: list[dict]) -> None:
    _notifications.send_consolidated_email(
        db,
        entries,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
    )


def queue_monitor_task(task_id: int) -> bool:
    """Enqueue a task for serial execution (manual trigger or initial run after add/import).

    Returns False if the task is already queued or running.
    """
    if not _acquire_task_slot(task_id):
        logger.warning("Task %s is already queued or running, skipping.", task_id)
        return False
    with _task_state_lock:
        _queued_task_ids.append(task_id)
    _execution_queue.put(task_id)
    logger.info("Task %s enqueued.", task_id)
    return True


def force_stop_queue() -> tuple[int | None, int]:
    """Force stop running/queued tasks and clear the execution queue.

    Returns (running_task_id, cleared_count).
    """
    with _force_stop_lock:
        with _task_state_lock:
            running_id = _running_task_id
            queued_ids = list(_queued_task_ids)
            _queued_task_ids.clear()
            for tid in queued_ids:
                _inflight_task_ids.discard(tid)

        drained = 0
        while True:
            try:
                item = _execution_queue.get_nowait()
            except queue.Empty:
                break
            if item is not None:
                drained += 1
            _execution_queue.task_done()

        if running_id is not None:
            _abort_running_scrape(running_id)

        cleared = max(len(queued_ids), drained)
        logger.warning(
            "Force stop requested: running=%s, queued_cleared=%d, drained=%d.",
            running_id,
            len(queued_ids),
            drained,
        )
        return running_id, cleared


def _abort_running_scrape(task_id: int) -> None:
    """Best-effort abort of the current scrape by killing Chromium."""
    try:
        from scraper import request_cancel_scrape, clear_cancel_scrape, _kill_zombie_browsers
        request_cancel_scrape()
        threading.Timer(60.0, clear_cancel_scrape).start()
        _kill_zombie_browsers()
        logger.warning("Attempted to abort running task %s by killing Chromium.", task_id)
    except Exception as exc:
        logger.warning("Failed to abort running task %s: %s", task_id, exc)


def _queue_worker() -> None:
    """Single background thread that drains _execution_queue one task at a time.

    This guarantees only one scrape is in flight at any moment, which keeps
    Amazon's anti-bot systems from flagging bursts of parallel requests.
    After the queue drains, any accumulated digest entries are flushed as one email.
    """
    global _running_task_id
    logger.info("Serial task-execution worker started.")
    while True:
        task_id = _execution_queue.get()
        if task_id is None:          # shutdown sentinel
            _flush_digest()
            _execution_queue.task_done()
            logger.info("Serial task-execution worker stopping.")
            break
        logger.info("Worker picking up task %s.", task_id)
        with _task_state_lock:
            _running_task_id = task_id
            if task_id in _queued_task_ids:
                _queued_task_ids.remove(task_id)
        try:
            _execute_monitor_task_locked(task_id)
        except Exception as exc:
            logger.error("Unhandled error in queue worker for task %s: %s", task_id, exc)
        finally:
            with _task_state_lock:
                _running_task_id = None
            _release_task_slot(task_id)
            _execution_queue.task_done()
            # Reschedule: next run = now + interval (dynamic timing).
            _reschedule_after_run(task_id)

        # If no more tasks are queued, flush the accumulated digest now.
        if _execution_queue.empty():
            _flush_digest()
        else:
            # Throttle between consecutive tasks to avoid anti-bot detection.
            delay = random.uniform(*_INTER_TASK_DELAY)
            logger.info("Inter-task delay: %.1fs before next task.", delay)
            time.sleep(delay)


def _is_queue_worker_thread() -> bool:
    return threading.current_thread().name == "task-queue-worker"


def _reschedule_after_run(task_id: int) -> None:
    """Reschedule a task so the next run is exactly `interval` hours from now."""
    db = SessionLocal()
    try:
        task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
        if task and task.is_active:
            schedule_task(task)
    finally:
        db.close()


def schedule_task(task: MonitorTask):
    job_id = f"task_{task.id}"

    existing = scheduler.get_job(job_id)
    if existing:
        scheduler.remove_job(job_id)

    if task.is_active:
        next_time = datetime.datetime.now() + datetime.timedelta(hours=task.check_interval_hours)
        scheduler.add_job(
            execute_monitor_task,
            "date",
            run_date=next_time,
            id=job_id,
            args=[task.id],
            misfire_grace_time=300,
        )
        logger.info(
            "Scheduled %s to run at %s (%sh from now).",
            job_id,
            next_time.strftime("%Y-%m-%d %H:%M:%S"),
            task.check_interval_hours,
        )


def remove_scheduled_task(task_id: int):
    job_id = f"task_{task_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        logger.info("Removed scheduled task %s.", job_id)


def init_scheduler():
    global _worker_thread
    with _scheduler_init_lock:
        if _worker_thread is not None and _worker_thread.is_alive() and scheduler.running:
            logger.info("Scheduler already initialized.")
            return

        db = SessionLocal()
        try:
            tasks = db.query(MonitorTask).filter(MonitorTask.is_active.is_(True)).all()
            for task in tasks:
                schedule_task(task)
            scheduler.add_job(
                prune_removed_products_history,
                "interval",
                hours=24,
                id=CLEANUP_JOB_ID,
                replace_existing=True,
                misfire_grace_time=1800,
                coalesce=True,
            )
            if not scheduler.running:
                scheduler.start()

            if _worker_thread is None or not _worker_thread.is_alive():
                _worker_thread = threading.Thread(
                    target=_queue_worker,
                    daemon=True,
                    name="task-queue-worker",
                )
                _worker_thread.start()

            prune_removed_products_history()
            logger.info("Scheduler started.")
        finally:
            db.close()


def shutdown_scheduler(wait: bool = False) -> None:
    global _worker_thread
    with _scheduler_shutdown_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            force_stop_queue()
            _execution_queue.put(None)
            _worker_thread.join(timeout=10 if wait else 2)
            if _worker_thread.is_alive():
                logger.warning("Task queue worker did not stop before shutdown timeout.")
            else:
                _worker_thread = None

        if scheduler.running:
            scheduler.shutdown(wait=wait)
