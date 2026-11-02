import datetime
import logging
import os
import queue
import random
import threading
import time
import uuid

from apscheduler.schedulers.background import BackgroundScheduler

from database import SessionLocal
from models import Category, MonitorTask, ProductItem, SystemConfig
import scheduler_notifications as _notifications
from scheduler_health import (
    handle_empty_scrape_result as _handle_empty_scrape_result_impl,
    handle_successful_scrape as _handle_successful_scrape_impl,
)
from utils import get_resource_path, probe_http_text, response_looks_blocked
try:
    from scraper import (
        fetch_products_for_task,
        ScrapeCancelled,
        ScrapeIncomplete,
        ScrapeTransientError,
        clear_pending_integrity_review,
        get_pending_integrity_review,
    )
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
    clear_pending_integrity_review = _mod.clear_pending_integrity_review
    get_pending_integrity_review = _mod.get_pending_integrity_review
from security import validate_monitor_target_url

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()
_execution_context = threading.local()

_task_state_lock = threading.Lock()
_inflight_task_ids: set[int] = set()
_running_task_id: int | None = None  # The single task currently being executed.
_queued_task_ids: list[int] = []     # Ordered list of task IDs waiting to run.
_task_digest_scopes: dict[int, dict] = {}
_manual_batch_remaining: dict[str, set[int]] = {}

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
NETWORK_CHECK_JOB_ID = "network_check_retry"
DAILY_STATUS_JOB_ID = "daily_status_report"
DAILY_STATUS_WATCHDOG_JOB_ID = "daily_status_report_watchdog"
_INTER_TASK_DELAY = (3, 8)     # Random delay (seconds) between consecutive tasks.
_SCRAPE_TIMEOUT = 600          # Max seconds for a single scrape attempt.
_SMTP_RETRY_ATTEMPTS = 0      # Route/port failover is immediate; outbox retries are delayed.
REPORT_OUTBOX_RETRY_JOB_ID = "report_outbox_retry"
_REPORT_OUTBOX_RETRY_MINUTES = max(
    1, int(os.getenv("MONITOR_WEB_EMAIL_RETRY_CHECK_MINUTES", "5") or "5")
)
_TASK_FAILURE_THRESHOLD = max(
    EMPTY_ALERT_THRESHOLD,
    int(os.getenv("MONITOR_WEB_TASK_FAILURE_THRESHOLD", "5") or "5"),
)
_NETWORK_CHECK_INTERVAL_MINUTES = 30  # Check network every 30 minutes when tasks are pending retry
_NETWORK_ALERT_POPUP_COOLDOWN_SECONDS = max(
    60, int(os.getenv("MONITOR_WEB_NETWORK_ALERT_COOLDOWN_SECONDS", "900") or "900")
)
_DAILY_STATUS_REPORT_HOUR = max(
    0, min(23, int(os.getenv("MONITOR_WEB_DAILY_REPORT_HOUR", "9") or "9"))
)
_DAILY_STATUS_REPORT_MINUTE = max(
    0, min(59, int(os.getenv("MONITOR_WEB_DAILY_REPORT_MINUTE", "0") or "0"))
)
_DAILY_STATUS_WATCHDOG_MINUTES = max(
    5, int(os.getenv("MONITOR_WEB_DAILY_REPORT_WATCHDOG_MINUTES", "30") or "30")
)
_DAILY_REPORT_TZ = datetime.timezone(datetime.timedelta(hours=8))
_NETWORK_TEST_TARGETS = [
    ("https://www.amazon.com", "Amazon 主站"),
    ("https://www.amazon.com/robots.txt", "Amazon robots.txt"),
]
_NETWORK_TEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

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


def build_all_tasks_digest_scope() -> dict:
    return {"key": "all", "label": "全部任务", "kind": "all"}


def build_group_digest_scope(
    group_id: int | None,
    group_name: str | None,
    *,
    unique_run: bool = False,
) -> dict:
    normalized_name = (group_name or "").strip() or "未分组"
    normalized_id = group_id if group_id is not None else "ungrouped"
    scope_key = f"group:{normalized_id}"
    if unique_run:
        scope_key = f"{scope_key}:run:{uuid.uuid4().hex}"
    return {
        "key": scope_key,
        "label": normalized_name,
        "kind": "group",
    }


def build_task_digest_scope(task_id: int, task_name: str | None) -> dict:
    normalized_name = (task_name or "").strip() or f"任务 {task_id}"
    return {
        "key": f"task:{task_id}:run:{uuid.uuid4().hex}",
        "label": normalized_name,
        "kind": "task",
    }


def _scheduled_digest_scope() -> dict:
    return {"key": "scheduled:daily", "label": "每日上新报告", "kind": "scheduled"}


def _default_digest_scope_for_task(task: MonitorTask) -> dict:
    group_name = ""
    if getattr(task, "group", None) is not None and task.group is not None:
        group_name = task.group.name or ""
    return build_group_digest_scope(task.category_id, group_name)


def _set_task_digest_scope(task_id: int, digest_scope: dict | None) -> None:
    if not digest_scope:
        return
    with _task_state_lock:
        _task_digest_scopes[task_id] = dict(digest_scope)


def _get_task_digest_scope(task_id: int) -> dict | None:
    with _task_state_lock:
        scope = _task_digest_scopes.get(task_id)
    return dict(scope) if scope else None


def _clear_task_digest_scope(task_id: int) -> None:
    with _task_state_lock:
        _task_digest_scopes.pop(task_id, None)


def _set_execution_context(task_id: int, digest_scope: dict | None) -> None:
    _execution_context.task_id = task_id
    _execution_context.digest_scope = dict(digest_scope) if digest_scope else None
    _execution_context.started_at = datetime.datetime.now(datetime.timezone.utc)


def _clear_execution_context() -> None:
    _execution_context.task_id = None
    _execution_context.digest_scope = None
    _execution_context.started_at = None


def _current_digest_scope(task: MonitorTask) -> dict:
    scope = getattr(_execution_context, "digest_scope", None)
    if scope:
        return dict(scope)
    scope = _get_task_digest_scope(task.id)
    if scope:
        return scope
    return _default_digest_scope_for_task(task)


def _current_execution_started_at() -> datetime.datetime | None:
    return getattr(_execution_context, "started_at", None)


def _clear_manual_review_state(task: MonitorTask) -> None:
    task.manual_review_pending = False
    task.manual_review_kind = None
    task.manual_review_scope_key = None
    task.manual_review_scope_label = None
    task.manual_review_scope_kind = None
    task.manual_review_started_at = None


def _mark_manual_review_pending(
    task: MonitorTask,
    *,
    reason: str,
    kind: str,
    digest_scope: dict | None = None,
    started_at: datetime.datetime | None = None,
) -> None:
    scope = dict(digest_scope or _current_digest_scope(task) or {})
    task.manual_review_pending = True
    task.manual_review_kind = (kind or "failure").strip() or "failure"
    task.manual_review_scope_key = scope.get("key") or f"group:{task.category_id or 'ungrouped'}"
    task.manual_review_scope_label = scope.get("label") or (task.group.name if getattr(task, "group", None) else "未分组")
    task.manual_review_scope_kind = scope.get("kind") or "group"
    task.manual_review_started_at = started_at or _current_execution_started_at() or datetime.datetime.now(datetime.timezone.utc)
    task.last_failure_reason = (reason or "").strip()[:500] or "抓取失败"


def _pending_manual_review_scopes() -> set[str]:
    db = SessionLocal()
    try:
        rows = (
            db.query(MonitorTask.manual_review_scope_key)
            .filter(
                MonitorTask.manual_review_pending.is_(True),
                MonitorTask.is_active.is_(True),
            )
            .all()
        )
        return {
            (row[0] or "").strip()
            for row in rows
            if row and (row[0] or "").strip()
        }
    finally:
        db.close()


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
        "检测到抓取网络或访问异常，建议切换网络、重连代理/VPN 后人工决定是否重试。"
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
    _last_network_recovery_message = (message or "").strip() or "网络访问已恢复，可人工重试之前受影响的抓取任务。"


def _clear_network_issue(recovered: bool = False, message: str = "") -> None:
    global _network_issue_active
    was_active = _network_issue_active
    _network_issue_active = False
    if recovered and was_active:
        _mark_network_recovery(message)


def add_to_retry_queue(task_id: int) -> None:
    """Legacy helper for marking tasks that need manual follow-up after network issues."""
    with _retry_queue_lock:
        if task_id not in _network_retry_queue:
            _network_retry_queue.append(task_id)
            _mark_network_issue(
                "检测到抓取网络或访问异常，部分任务已加入待人工处理列表。"
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


def remove_from_retry_queue(task_id: int) -> bool:
    removed = False
    with _retry_queue_lock:
        while task_id in _network_retry_queue:
            _network_retry_queue.remove(task_id)
            removed = True
        if removed and not _network_retry_queue:
            _clear_network_issue()
    return removed


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


def run_network_diagnostics() -> dict:
    """Run the canonical Amazon connectivity check used by both polling and retry logic."""
    results = {
        "success": False,
        "message": "",
        "response_time_ms": None,
        "details": [],
    }
    total_elapsed = 0
    accessible_count = 0
    all_ok = True

    for url, name in _NETWORK_TEST_TARGETS:
        probe = probe_http_text(
            url,
            headers=_NETWORK_TEST_HEADERS,
            timeout=15,
            max_bytes=2048,
        )
        if probe.error_kind == "timeout":
            results["details"].append(
                {
                    "name": name,
                    "status": "❌ 超时",
                    "time_ms": None,
                    "note": "连接超时 (>15s)",
                }
            )
            all_ok = False
            continue
        if probe.error_kind:
            results["details"].append(
                {
                    "name": name,
                    "status": "❌ 失败",
                    "time_ms": None,
                    "note": (probe.error_message or "")[:80],
                }
            )
            all_ok = False
            continue

        elapsed = round(probe.elapsed_ms or 0)
        total_elapsed += elapsed
        status = probe.status_code or 0
        final_url = probe.final_url or url
        if response_looks_blocked(final_url, probe.body_text):
            results["details"].append(
                {
                    "name": name,
                    "status": "⚠️ 可能被限制",
                    "time_ms": elapsed,
                    "note": "检测到验证码/机器人检查",
                }
            )
            all_ok = False
            continue

        if 200 <= status < 400:
            accessible_count += 1
            label = "✅ 正常" if status == 200 else "✅ 可访问"
            note = "" if status == 200 else f"HTTP {status}"
            results["details"].append(
                {
                    "name": name,
                    "status": label,
                    "time_ms": elapsed,
                    "note": note,
                }
            )
            continue

        all_ok = False
        results["details"].append(
            {
                "name": name,
                "status": "⚠️ 服务异常" if status >= 500 else "⚠️ 状态异常",
                "time_ms": elapsed,
                "note": f"HTTP {status}",
            }
        )

    if results["details"]:
        results["response_time_ms"] = round(total_elapsed / len(results["details"]))

    is_healthy = accessible_count == len(_NETWORK_TEST_TARGETS) and all_ok
    avg_time = results["response_time_ms"] or 0
    if is_healthy and avg_time >= 2000:
        is_healthy = False
        results["message"] = f"⚠️ 网络很慢，平均响应 {avg_time}ms，建议稍后再试。"
    elif is_healthy and avg_time >= 500:
        results["message"] = f"⚠️ 网络较慢，平均响应 {avg_time}ms，可以抓取但可能较慢。"
    elif is_healthy:
        results["message"] = f"✅ 网络状态良好！平均响应 {avg_time}ms，适合抓取。"
    else:
        results["message"] = "❌ 网络连接异常，请检查代理设置或稍后再试。"

    results["success"] = is_healthy
    return results


def _check_network_health() -> bool:
    return bool(run_network_diagnostics().get("success"))


def _requeue_pending_network_tasks(recovery_message: str) -> int:
    with _retry_queue_lock:
        tasks_to_retry = list(_network_retry_queue)
        _network_retry_queue.clear()
        _clear_network_issue(recovered=True, message=recovery_message)

    for task_id in tasks_to_retry:
        queue_monitor_task(task_id, digest_scope=_scheduled_digest_scope())
        logger.info("[RETRY] Task %s requeued for execution.", task_id)
    return len(tasks_to_retry)


def run_manual_network_check() -> dict:
    """Run diagnostics on demand without triggering any automatic retries."""
    global _network_healthy, _last_network_check
    diagnostics = run_network_diagnostics()
    _last_network_check = datetime.datetime.now(datetime.timezone.utc)
    _network_healthy = bool(diagnostics.get("success"))

    with _retry_queue_lock:
        pending_count = len(_network_retry_queue)

    if diagnostics["success"] and pending_count:
        _clear_network_issue(
            recovered=True,
            message=f"网络访问已恢复，可人工重试 {pending_count} 个待处理任务。",
        )
        diagnostics["message"] += f" 当前仍有 {pending_count} 个待人工处理任务，请确认后手动重试。"
    elif not diagnostics["success"] and pending_count:
        _mark_network_issue(
            "检测到当前网络无法稳定访问 Amazon，建议切换网络或重新连接代理/VPN。"
        )

    return diagnostics


def _network_check_and_retry() -> None:
    """Legacy periodic network check that never requeues tasks automatically."""
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
    
    diagnostics = run_network_diagnostics()
    is_healthy = bool(diagnostics.get("success"))
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
    
    logger.info("[RETRY] Network healthy. %d tasks still require manual retry.", pending_count)
    _clear_network_issue(
        recovered=True,
        message=f"网络访问已恢复，可人工重试 {pending_count} 个待处理任务。",
    )


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
            "cooldown_seconds": _NETWORK_ALERT_POPUP_COOLDOWN_SECONDS,
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


def _send_daily_status_email(db, report: dict) -> bool:
    return _notifications.send_daily_status_email(
        db,
        report,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
    )


def execute_monitor_task(task_id: int, digest_scope: dict | None = None):
    """Compatibility entry point: enqueue one manually reported task."""
    queue_monitor_task(task_id, digest_scope=digest_scope)


def _fetch_with_retry(db, task_id: int, task_name: str):
    """Run a single scrape attempt. Any retry must be triggered manually by the user."""
    try:
        return fetch_products_for_task(db, task_id)
    except ScrapeCancelled:
        logger.warning("Task %s was cancelled, not retrying.", task_name)
        raise


def _execute_monitor_task_locked(task_id: int):
    logger.info("Executing scheduled task %s", task_id)
    db = SessionLocal()
    task: MonitorTask | None = None
    try:
        task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
        if not task or not task.is_active:
            return

        _set_execution_context(task_id, _current_digest_scope(task))

        if not _validate_task_url(task_id, task.url):
            _mark_task_failure(db, task, "任务 URL 未通过安全校验")
            return

        current, new_prods, removed_prods = _fetch_with_retry(db, task_id, task.name)
        is_first_successful_run = task.last_run_at is None and bool(current)

        if not current:
            _handle_empty_scrape_result(db, task)
            return

        _handle_successful_scrape(db, task, current, new_prods, removed_prods, is_first_successful_run)

    except ScrapeCancelled:
        logger.warning("Task %s cancelled by user.", task_id)
        if task is not None:
            _queue_digest_entry(
                task,
                [],
                [],
                status="cancelled",
                status_message="任务已由用户取消",
            )
        return
    except Exception as exc:
        from scraper import ScrapeIncomplete, ScrapeTransientError
        if isinstance(exc, (ScrapeIncomplete, ScrapeTransientError)):
            logger.warning(
                "Task %s hit a transient scrape failure - waiting for manual confirmation.",
                task_id
            )
            if task is not None:
                review_kind = "integrity" if get_pending_integrity_review(task.id) else "failure"
                _mark_task_failure(db, task, str(exc), manual_review_kind=review_kind)
            return
        logger.error("Error executing task %s: %s", task_id, exc)
        if task is not None:
            _mark_task_failure(db, task, str(exc))
    finally:
        _clear_execution_context()
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


def _mark_task_failure(
    db,
    task: MonitorTask,
    reason: str,
    *,
    manual_review_kind: str = "failure",
) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    task.consecutive_failure_count = (task.consecutive_failure_count or 0) + 1
    task.last_failure_at = now
    _mark_manual_review_pending(
        task,
        reason=reason,
        kind=manual_review_kind,
        started_at=_current_execution_started_at(),
    )
    if task.consecutive_failure_count >= _TASK_FAILURE_THRESHOLD:
        task.health_state = "failed"
    elif task.health_state == "healthy":
        task.health_state = "warning"
    db.commit()
    _queue_digest_entry(
        task,
        [],
        [],
        status="failed",
        status_message=task.last_failure_reason or reason,
    )


def _handle_empty_scrape_result(db, task) -> None:
    _mark_manual_review_pending(
        task,
        reason="抓取结果为空",
        kind="failure",
        started_at=_current_execution_started_at(),
    )
    _handle_empty_scrape_result_impl(
        db,
        task,
        empty_alert_threshold=EMPTY_ALERT_THRESHOLD,
        failure_threshold=_TASK_FAILURE_THRESHOLD,
    )
    _queue_digest_entry(
        task,
        [],
        [],
        status="failed",
        status_message=task.last_failure_reason or "抓取结果为空",
    )


def _handle_successful_scrape(db, task, current, new_prods, removed_prods, is_first_successful_run: bool) -> None:
    clear_pending_integrity_review(task.id)
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
    task: MonitorTask,
    new_products: list,
    removed_products: list,
    is_baseline: bool = False,
    baseline_count: int = 0,
    current_products: list | None = None,
    status: str = "success",
    status_message: str = "",
) -> None:
    digest_scope = _current_digest_scope(task)
    _notifications.queue_digest_entry(
        task,
        new_products,
        removed_products,
        is_baseline=is_baseline,
        baseline_count=baseline_count,
        digest_scope=digest_scope,
        started_at=_current_execution_started_at(),
        current_products=current_products,
        status=status,
        status_message=status_message,
    )


def _flush_digest(scope_keys: set[str] | None = None) -> None:
    # A normal manual run is also the recovery trigger for any older failed
    # reports. Excluding the active scope avoids retrying the same report twice
    # when its first delivery attempt is about to run below.
    _notifications.retry_pending_reports(
        SessionLocal,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
        exclude_scope_keys=scope_keys,
    )
    _notifications.flush_digest(
        SessionLocal,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
        only_scope_keys=scope_keys,
    )


def flush_pending_digests() -> None:
    _flush_digest()


def get_report_delivery_status(db, page: int = 1) -> dict:
    return _notifications.get_report_delivery_status(db, page=page)


def retry_report_delivery_now() -> dict[str, int]:
    """Retry deliverable outbox rows once, without stacking concurrent requests."""
    return _notifications.retry_pending_reports(
        SessionLocal,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
        wait_for_lock=False,
    )


def retry_report_delivery_by_id(report_id: int) -> dict[str, int]:
    """Retry one selected durable report without stacking concurrent requests."""
    return _notifications.retry_report_by_id(
        SessionLocal,
        report_id,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
        wait_for_lock=False,
    )


def resolve_manual_review_report(
    scope_key: str | None,
    task_id: int,
    *,
    resolution_message: str = "人工确认已完成",
) -> dict[str, object]:
    """Update and release the durable report associated with a reviewed task."""
    return _notifications.resolve_held_report_task(
        SessionLocal,
        scope_key,
        task_id,
        resolution_message=resolution_message,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
    )


def _send_consolidated_email(db, entries: list[dict]) -> None:
    _notifications.send_consolidated_email(
        db,
        entries,
        retry_attempts=_SMTP_RETRY_ATTEMPTS,
    )


def _task_status_payload(task: MonitorTask) -> dict:
    group_name = ""
    if getattr(task, "group", None) is not None and task.group is not None:
        group_name = task.group.name or ""
    if task.manual_review_pending:
        status_text = "待人工确认"
    elif task.health_state == "failed":
        status_text = f"失败 {task.consecutive_failure_count or 0}/{_TASK_FAILURE_THRESHOLD}"
    elif task.health_state == "alert":
        status_text = f"空抓告警 {task.consecutive_empty_count or 0}/{EMPTY_ALERT_THRESHOLD}"
    elif task.health_state == "warning":
        if task.consecutive_failure_count:
            status_text = f"异常 {task.consecutive_failure_count}/{_TASK_FAILURE_THRESHOLD}"
        else:
            status_text = f"空抓预警 {task.consecutive_empty_count or 0}/{EMPTY_ALERT_THRESHOLD}"
    else:
        status_text = "正常"

    return {
        "task_id": task.id,
        "task_name": task.name,
        "group_name": group_name or "未分组",
        "status_text": status_text,
        "reason": task.last_failure_reason or "",
        "updated_at": (
            task.last_failure_at
            or task.last_health_alert_at
            or task.last_recovery_at
            or task.last_run_at
        ),
    }


def _daily_report_day_bounds(now: datetime.datetime) -> tuple[datetime.datetime, datetime.datetime, str]:
    local_now = now.astimezone(_DAILY_REPORT_TZ)
    local_start = datetime.datetime.combine(local_now.date(), datetime.time.min, tzinfo=_DAILY_REPORT_TZ)
    local_end = local_start + datetime.timedelta(days=1)
    return (
        local_start.astimezone(datetime.timezone.utc).replace(tzinfo=None),
        local_end.astimezone(datetime.timezone.utc).replace(tzinfo=None),
        local_now.strftime("%Y-%m-%d"),
    )


def _as_utc_datetime(value: datetime.datetime | None) -> datetime.datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc)


def _daily_report_local_date(value: datetime.datetime | None) -> datetime.date | None:
    utc_value = _as_utc_datetime(value)
    if utc_value is None:
        return None
    return utc_value.astimezone(_DAILY_REPORT_TZ).date()


def _daily_report_sent_for_date(config: SystemConfig | None, report_date: datetime.date) -> bool:
    if not config or not config.last_daily_report_at:
        return False
    return _daily_report_local_date(config.last_daily_report_at) == report_date


def _daily_report_scheduled_at(report_date: datetime.date) -> datetime.datetime:
    return datetime.datetime.combine(
        report_date,
        datetime.time(_DAILY_STATUS_REPORT_HOUR, _DAILY_STATUS_REPORT_MINUTE),
        tzinfo=_DAILY_REPORT_TZ,
    )


def _daily_report_is_due(now: datetime.datetime) -> bool:
    utc_now = _as_utc_datetime(now) or datetime.datetime.now(datetime.timezone.utc)
    local_now = utc_now.astimezone(_DAILY_REPORT_TZ)
    return local_now >= _daily_report_scheduled_at(local_now.date())


def _format_daily_product_time(value) -> str:
    if not value:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(_DAILY_REPORT_TZ).strftime("%m-%d %H:%M")


def _daily_new_product_payload(db, now: datetime.datetime) -> tuple[list[dict], str]:
    start_utc, end_utc, report_date_text = _daily_report_day_bounds(now)
    rows = (
        db.query(
            ProductItem.name,
            ProductItem.product_link,
            ProductItem.discovered_at,
            MonitorTask.name.label("task_name"),
            Category.name.label("group_name"),
        )
        .join(MonitorTask, MonitorTask.id == ProductItem.task_id)
        .outerjoin(Category, Category.id == MonitorTask.category_id)
        .filter(
            ProductItem.removed_at.is_(None),
            ProductItem.is_new_discovery.is_(True),
            ProductItem.discovered_at.isnot(None),
            ProductItem.discovered_at >= start_utc,
            ProductItem.discovered_at < end_utc,
        )
        .order_by(
            Category.name.asc(),
            MonitorTask.name.asc(),
            ProductItem.discovered_at.desc(),
            ProductItem.name.asc(),
        )
        .all()
    )
    return [
        {
            "name": row.name or "",
            "link": row.product_link or "",
            "task_name": row.task_name or "未命名任务",
            "group_name": row.group_name or "未分组",
            "discovered_at": row.discovered_at,
            "discovered_text": _format_daily_product_time(row.discovered_at),
        }
        for row in rows
    ], report_date_text


def send_daily_status_report(now: datetime.datetime | None = None) -> None:
    db = SessionLocal()
    try:
        now = _as_utc_datetime(now) or datetime.datetime.now(datetime.timezone.utc)
        report_date = now.astimezone(_DAILY_REPORT_TZ).date()
        config = db.query(SystemConfig).first()
        if _daily_report_sent_for_date(config, report_date):
            logger.info("Daily status report already sent for %s, skipping.", report_date)
            return

        tasks = (
            db.query(MonitorTask)
            .outerjoin(Category, Category.id == MonitorTask.category_id)
            .all()
        )
        active_tasks = [task for task in tasks if task.is_active]
        abnormal_tasks = [
            task for task in active_tasks
            if task.manual_review_pending or task.health_state in {"warning", "alert"} or task.consecutive_failure_count
        ]
        failed_tasks = [
            task for task in active_tasks
            if task.health_state == "failed" or (task.consecutive_failure_count or 0) >= _TASK_FAILURE_THRESHOLD
        ]
        abnormal_tasks = [task for task in abnormal_tasks if task not in failed_tasks]

        running_id, waiting_ids = get_queue_snapshot()
        running_task_name = ""
        if running_id:
            running_task = next((task for task in tasks if task.id == running_id), None)
            running_task_name = running_task.name if running_task else f"Task-{running_id}"

        report = {
            "generated_at": now,
            "report_date_text": "",
            "active_task_count": len(active_tasks),
            "retry_pending_count": sum(1 for task in active_tasks if task.manual_review_pending),
            "running_task_name": running_task_name,
            "waiting_count": len(waiting_ids),
            "abnormal_tasks": [_task_status_payload(task) for task in abnormal_tasks],
            "failed_tasks": [_task_status_payload(task) for task in failed_tasks],
            "new_products": [],
        }
        report["new_products"], report["report_date_text"] = _daily_new_product_payload(db, now)
        sent = _send_daily_status_email(db, report)

        if sent and config is not None:
            config.last_daily_report_at = now
            db.commit()
    except Exception as exc:
        logger.error("Failed to send daily status report: %s", exc)
    finally:
        db.close()


def send_daily_status_report_if_due(now: datetime.datetime | None = None) -> None:
    now = _as_utc_datetime(now) or datetime.datetime.now(datetime.timezone.utc)
    local_now = now.astimezone(_DAILY_REPORT_TZ)
    if not _daily_report_is_due(now):
        logger.info(
            "Daily status report is not due yet for %s; scheduled at %s.",
            local_now.date(),
            _daily_report_scheduled_at(local_now.date()).strftime("%H:%M"),
        )
        return
    send_daily_status_report(now=now)


def queue_monitor_batch(task_ids: list[int], digest_scope: dict) -> int:
    """Atomically register and enqueue one manually started report batch."""
    scope = dict(digest_scope or {})
    scope_key = (scope.get("key") or "").strip()
    if not scope_key:
        raise ValueError("A manual batch requires a report scope key.")

    normalized_ids = list(dict.fromkeys(int(task_id) for task_id in task_ids))
    if not normalized_ids:
        return 0

    queued_ids: list[int] = []
    with _task_state_lock:
        for task_id in normalized_ids:
            if task_id in _inflight_task_ids:
                logger.warning("Task %s is already queued or running, skipping.", task_id)
                continue
            _inflight_task_ids.add(task_id)
            _task_digest_scopes[task_id] = dict(scope)
            _queued_task_ids.append(task_id)
            queued_ids.append(task_id)

        if queued_ids:
            _manual_batch_remaining.setdefault(scope_key, set()).update(queued_ids)

    for task_id in queued_ids:
        remove_from_retry_queue(task_id)
        _execution_queue.put(task_id)

    if queued_ids:
        logger.info(
            "Manual report batch %s enqueued with %d task(s).",
            scope_key,
            len(queued_ids),
        )
    return len(queued_ids)


def queue_monitor_task(task_id: int, digest_scope: dict | None = None) -> bool:
    """Enqueue one manually started task and send its completion report."""
    scope = dict(digest_scope or {})
    if not scope:
        scope = build_task_digest_scope(task_id, None)
    return queue_monitor_batch([task_id], scope) == 1


def _complete_manual_batch_task(task_id: int, digest_scope: dict | None) -> str | None:
    scope_key = ((digest_scope or {}).get("key") or "").strip()
    if not scope_key:
        return None
    with _task_state_lock:
        remaining = _manual_batch_remaining.get(scope_key)
        if remaining is None:
            return None
        remaining.discard(task_id)
        if remaining:
            return None
        _manual_batch_remaining.pop(scope_key, None)
    return scope_key


def force_stop_queue() -> tuple[int | None, int]:
    """Force stop running/queued tasks and clear the execution queue.

    Returns (running_task_id, cleared_count).
    """
    with _force_stop_lock:
        cancelled_batch_tasks: list[tuple[int, dict | None]] = []
        with _task_state_lock:
            running_id = _running_task_id
            queued_ids = list(_queued_task_ids)
            _queued_task_ids.clear()
            for tid in queued_ids:
                _inflight_task_ids.discard(tid)
                cancelled_batch_tasks.append((tid, _task_digest_scopes.pop(tid, None)))

        completed_scope_keys = {
            scope_key
            for tid, scope in cancelled_batch_tasks
            if (scope_key := _complete_manual_batch_task(tid, scope))
        }

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
        elif completed_scope_keys:
            _flush_digest(completed_scope_keys)

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
    Each explicit manual batch is flushed as soon as its final task completes.
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
        digest_scope = _get_task_digest_scope(task_id)
        with _task_state_lock:
            _running_task_id = task_id
            if task_id in _queued_task_ids:
                _queued_task_ids.remove(task_id)
        try:
            _set_execution_context(task_id, digest_scope)
            _execute_monitor_task_locked(task_id)
        except Exception as exc:
            logger.error("Unhandled error in queue worker for task %s: %s", task_id, exc)
        finally:
            _clear_execution_context()
            with _task_state_lock:
                _running_task_id = None
            completed_scope_key = _complete_manual_batch_task(task_id, digest_scope)
            if completed_scope_key:
                _flush_digest({completed_scope_key})
            _release_task_slot(task_id)
            _clear_task_digest_scope(task_id)
            _execution_queue.task_done()

        try:
            if not _execution_queue.empty():
                # Throttle between consecutive tasks to avoid anti-bot detection.
                delay = random.uniform(*_INTER_TASK_DELAY)
                logger.info("Inter-task delay: %.1fs before next task.", delay)
                time.sleep(delay)
        except Exception as exc:
            logger.error("Queue worker post-task housekeeping failed: %s", exc)


def _is_queue_worker_thread() -> bool:
    return threading.current_thread().name == "task-queue-worker"


def _reschedule_after_run(task_id: int) -> None:
    """Automatic rescheduling is intentionally disabled in manual-only mode."""
    logger.debug("Task %s completed; automatic rescheduling is disabled.", task_id)


def schedule_task(task: MonitorTask):
    """Compatibility no-op: tasks run only after an explicit user action."""
    job_id = f"task_{task.id}"
    existing = scheduler.get_job(job_id)
    if existing:
        scheduler.remove_job(job_id)
    logger.debug("Task %s remains manual-only; no scheduler job created.", task.id)


def remove_scheduled_task(task_id: int):
    job_id = f"task_{task_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        logger.info("Removed scheduled task %s.", job_id)


TASK_JOB_REPAIR_JOB_ID = "task_job_repair_watchdog"


def _repair_missing_task_jobs() -> None:
    """Watchdog: re-create scheduler jobs for active tasks that lost theirs.

    A task's one-shot "date" job can vanish without a trace (errors during
    reschedule, scheduler restarts). Without repair, that task silently never
    runs again while the app looks perfectly healthy.
    """
    db = SessionLocal()
    try:
        tasks = db.query(MonitorTask).filter(MonitorTask.is_active.is_(True)).all()
        now = datetime.datetime.now()
        repaired = 0
        for task in tasks:
            if task.manual_review_pending:
                continue
            job_id = f"task_{task.id}"
            if scheduler.get_job(job_id) is not None:
                continue
            with _task_state_lock:
                busy = task.id == _running_task_id or task.id in _queued_task_ids
            if busy:
                continue  # job re-created by _reschedule_after_run when done
            next_run = task.next_run_at
            if next_run is None or next_run <= now:
                # Overdue — run soon, staggered so repairs don't flood the queue.
                next_run = now + datetime.timedelta(minutes=2 + repaired * 3)
            scheduler.add_job(
                execute_monitor_task,
                "date",
                run_date=next_run,
                id=job_id,
                args=[task.id],
                misfire_grace_time=None,
            )
            repaired += 1
            logger.warning(
                "Repaired missing scheduler job for task %s (%s); next run at %s.",
                task.id,
                task.name,
                next_run.strftime("%Y-%m-%d %H:%M:%S"),
            )
    except Exception as exc:
        logger.error("Task-job repair watchdog failed: %s", exc)
    finally:
        db.close()


def _remove_automatic_jobs() -> None:
    automatic_job_ids = {
        DAILY_STATUS_JOB_ID,
        DAILY_STATUS_WATCHDOG_JOB_ID,
        TASK_JOB_REPAIR_JOB_ID,
    }
    for job in scheduler.get_jobs():
        if job.id.startswith("task_") or job.id in automatic_job_ids:
            scheduler.remove_job(job.id)
            logger.info("Removed automatic job %s for manual-only mode.", job.id)


def init_scheduler():
    global _worker_thread
    with _scheduler_init_lock:
        _remove_automatic_jobs()
        if not scheduler.running:
            scheduler.start()

        if scheduler.get_job(REPORT_OUTBOX_RETRY_JOB_ID) is None:
            scheduler.add_job(
                _notifications.retry_pending_reports,
                "interval",
                minutes=_REPORT_OUTBOX_RETRY_MINUTES,
                id=REPORT_OUTBOX_RETRY_JOB_ID,
                args=[SessionLocal],
                kwargs={
                    "retry_attempts": _SMTP_RETRY_ATTEMPTS,
                    "respect_schedule": True,
                },
                max_instances=1,
                coalesce=True,
            )
            logger.info(
                "Durable email retry job enabled: interval_minutes=%d.",
                _REPORT_OUTBOX_RETRY_MINUTES,
            )

        if _worker_thread is not None and _worker_thread.is_alive():
            logger.info("Manual execution worker already initialized.")
            return

        if _worker_thread is None or not _worker_thread.is_alive():
            _worker_thread = threading.Thread(
                target=_queue_worker,
                daemon=True,
                name="task-queue-worker",
            )
            _worker_thread.start()

        # Delivery recovery is independent of task scheduling. Run it once in
        # the background so application startup is never blocked by SMTP.
        threading.Thread(
            target=lambda: _notifications.retry_pending_reports(
                SessionLocal,
                retry_attempts=_SMTP_RETRY_ATTEMPTS,
                respect_schedule=True,
            ),
            daemon=True,
            name="report-outbox-retry",
        ).start()

        logger.info("Manual-only execution worker started; automatic jobs are disabled.")


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
