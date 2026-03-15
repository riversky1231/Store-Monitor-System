import datetime
import logging
import os
import queue
import random
import smtplib
import threading
import time
from email.mime.text import MIMEText
from html import escape
from urllib.parse import urlparse

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import Session

from database import SessionLocal
from models import MonitorTask, ProductItem, SystemConfig
try:
    from scraper import fetch_products_for_task, ScrapeCancelled
except ModuleNotFoundError:
    # Fallback for packaged builds if module resolution fails.
    import importlib.util
    import sys as _sys
    from utils import get_resource_path

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
from security import decrypt_secret, encrypt_secret, validate_monitor_target_url, EMAIL_RE

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()

_task_state_lock = threading.Lock()
_inflight_task_ids: set[int] = set()
_running_task_id: int | None = None  # The single task currently being executed.
_queued_task_ids: list[int] = []     # Ordered list of task IDs waiting to run.

# Global serial execution queue — only one task runs at a time.
_execution_queue: queue.Queue[int | None] = queue.Queue()
_worker_thread: threading.Thread | None = None

# Accumulated notifications to be sent as one consolidated email.
_digest_lock = threading.Lock()
_pending_digest: list[dict] = []  # [{task_name, new_products, removed_products}]

_force_stop_lock = threading.Lock()

EMPTY_ALERT_THRESHOLD = 3  # Send health alert after this many consecutive 0-product scrapes.
DEFAULT_PRODUCT_RETENTION_DAYS = 90
CLEANUP_JOB_ID = "prune_removed_products"
_TASK_RETRY_ATTEMPTS = 2       # How many extra retries if scrape returns nothing.
_TASK_RETRY_DELAY = (10, 20)   # Random delay (seconds) between retries.
_INTER_TASK_DELAY = (3, 8)     # Random delay (seconds) between consecutive tasks.
_SCRAPE_TIMEOUT = 600          # Max seconds for a single scrape attempt.
_SMTP_RETRY_ATTEMPTS = 2      # Extra retries for email sending.


def get_inflight_task_ids() -> set[int]:
    with _task_state_lock:
        return set(_inflight_task_ids)


def get_queue_snapshot() -> tuple[int | None, list[int]]:
    """Return (currently_running_task_id, [waiting_task_ids])."""
    with _task_state_lock:
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


def _clean_subject_text(value: str) -> str:
    return (value or "").replace("\r", " ").replace("\n", " ").strip()


def _parse_recipients(raw_value: str) -> list[str]:
    recipients = []
    for item in (raw_value or "").split(","):
        if "\r" in item or "\n" in item:
            continue
        email = item.strip()
        if not email:
            continue
        if EMAIL_RE.fullmatch(email):
            recipients.append(email)
    return list(dict.fromkeys(recipients))


def _safe_link_for_html(link: str) -> str:
    candidate = (link or "").strip()
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "#"
    return escape(candidate, quote=True)


def _load_smtp_context(
    db: Session, task: MonitorTask
) -> tuple[SystemConfig, str, list[str]] | None:
    """Resolve and validate all prerequisites for sending email.

    Migrates a plaintext SMTP password to encrypted form as a side effect.
    Returns (config, smtp_password, recipients) or None if anything is missing.
    """
    config = db.query(SystemConfig).first()
    if not config or not config.sender_email:
        logger.error("SMTP sender email is not configured.")
        return None

    stored_password = config.sender_password or ""
    if stored_password and not stored_password.startswith("enc::"):
        try:
            config.sender_password = encrypt_secret(stored_password)
            db.commit()
            stored_password = config.sender_password
        except RuntimeError:
            logger.warning("SMTP password migration skipped: encryption backend unavailable.")
        except Exception as exc:
            logger.warning("SMTP password migration failed: %s", exc)

    smtp_password = os.getenv("STORE_MONITOR_SMTP_PASSWORD") or decrypt_secret(stored_password)
    if not smtp_password:
        logger.error("SMTP password is not configured.")
        return None

    recipients = _parse_recipients(task.recipients)
    if not recipients:
        logger.error("No valid recipients for task %s.", task.name)
        return None

    return config, smtp_password, recipients


def _render_product_table(
    products: list,
    header_bg: str,
    text_color: str,
    link_color: str,
) -> str:
    """Return an HTML table string for a list of products."""
    rows = []
    for p in products:
        safe_name = escape((p.get("name") or "").strip(), quote=False)
        safe_href = _safe_link_for_html(p.get("link") or "")
        rows.append(
            f"<tr>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;color:{text_color};'>{safe_name}</td>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;'>"
            f"<a href='{safe_href}' style='color:{link_color};'>查看商品</a></td></tr>"
        )
    return (
        f"<table style='width:100%;border-collapse:collapse;margin-bottom:20px;'>"
        f"<thead><tr style='background:{header_bg};text-align:left;'>"
        f"<th style='padding:10px;border:1px solid #e5e7eb;'>商品名称</th>"
        f"<th style='padding:10px;border:1px solid #e5e7eb;'>链接</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _smtp_send(config: SystemConfig, smtp_password: str, msg: MIMEText, recipients: list[str]) -> None:
    last_exc: Exception | None = None
    for attempt in range(1 + _SMTP_RETRY_ATTEMPTS):
        try:
            with smtplib.SMTP_SSL(config.smtp_server, config.smtp_port, timeout=30) as server:
                server.login(config.sender_email, smtp_password)
                server.send_message(msg, to_addrs=recipients)
            return
        except Exception as exc:
            last_exc = exc
            if attempt < _SMTP_RETRY_ATTEMPTS:
                wait = 5 * (attempt + 1)
                logger.warning("SMTP send failed (attempt %d/%d), retrying in %ds: %s",
                               attempt + 1, 1 + _SMTP_RETRY_ATTEMPTS, wait, exc)
                time.sleep(wait)
    if last_exc:
        raise last_exc


def send_email(db: Session, task: MonitorTask, new_products: list, removed_products: list | None = None):
    removed_products = removed_products or []
    ctx = _load_smtp_context(db, task)
    if not ctx:
        return
    config, smtp_password, recipients = ctx

    task_name = _clean_subject_text(task.name) or f"Task-{task.id}"
    parts = []
    if new_products:
        parts.append(f"{len(new_products)} 新上架")
    if removed_products:
        parts.append(f"{len(removed_products)} 已下架")
    subject = f"[Monitor] {task_name}: {', '.join(parts)}"

    safe_task_name = escape(task_name, quote=False)
    sections = [f"<div style='font-family:sans-serif;padding:20px;'><h2>店铺动态: {safe_task_name}</h2>"]
    if new_products:
        sections.append(f"<h3 style='color:#16a34a;'>新上架商品 ({len(new_products)})</h3>")
        sections.append(_render_product_table(new_products, "#f0fdf4", "#111111", "#4f46e5"))
    if removed_products:
        sections.append(f"<h3 style='color:#dc2626;'>已下架商品 ({len(removed_products)})</h3>")
        sections.append(_render_product_table(removed_products, "#fef2f2", "#6b7280", "#6b7280"))
    sections.append("</div>")

    msg = MIMEText("".join(sections), "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = config.sender_email
    msg["To"] = ", ".join(recipients)
    try:
        _smtp_send(config, smtp_password, msg, recipients)
        logger.info("Email sent for task %s to %d recipient(s).", task.name, len(recipients))
    except Exception as exc:
        logger.error("Failed to send email for task %s: %s", task.name, exc)


def _send_health_alert(db: Session, task: MonitorTask, consecutive_count: int) -> None:
    ctx = _load_smtp_context(db, task)
    if not ctx:
        logger.error("Cannot send health alert: SMTP not configured.")
        return
    config, smtp_password, recipients = ctx

    task_name = _clean_subject_text(task.name) or f"Task-{task.id}"
    safe_task_name = escape(task_name, quote=False)
    subject = f"[Monitor] WARNING: {task_name} has returned 0 products {consecutive_count} times in a row"
    body = (
        f"<div style='font-family:sans-serif;padding:20px;'>"
        f"<h2 style='color:#dc2626;'>Scraper Health Alert</h2>"
        f"<p>Task <b>{safe_task_name}</b> has returned <b>0 products</b> for "
        f"<b>{consecutive_count}</b> consecutive scrapes.</p>"
        f"<p>Possible causes:</p><ul>"
        f"<li>The target site is blocking the scraper (CAPTCHA / IP ban).</li>"
        f"<li>The target page URL has changed or is temporarily unavailable.</li>"
        f"<li>The CSS selector no longer matches any elements after a site redesign.</li>"
        f"</ul><p>Please check the application logs and verify the task configuration.</p>"
        f"<p style='color:#6b7280;font-size:12px;'>Task URL: {escape(task.url or '', quote=True)}</p>"
        f"</div>"
    )
    msg = MIMEText(body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = config.sender_email
    msg["To"] = ", ".join(recipients)
    try:
        _smtp_send(config, smtp_password, msg, recipients)
        logger.warning("Health alert sent for task %s after %d empty scrapes.", task.name, consecutive_count)
    except Exception as exc:
        logger.error("Failed to send health alert for task %s: %s", task.name, exc)


def _send_recovery_notification(db: Session, task: MonitorTask, prev_count: int) -> None:
    ctx = _load_smtp_context(db, task)
    if not ctx:
        return
    config, smtp_password, recipients = ctx

    task_name = _clean_subject_text(task.name) or f"Task-{task.id}"
    safe_task_name = escape(task_name, quote=False)
    subject = f"[Monitor] RESOLVED: {task_name} is healthy again"
    body = (
        f"<div style='font-family:sans-serif;padding:20px;'>"
        f"<h2 style='color:#16a34a;'>Scraper Recovered</h2>"
        f"<p>Task <b>{safe_task_name}</b> has successfully scraped products again "
        f"after <b>{prev_count}</b> consecutive empty runs.</p>"
        f"<p>The monitor is now operating normally.</p></div>"
    )
    msg = MIMEText(body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = config.sender_email
    msg["To"] = ", ".join(recipients)
    try:
        _smtp_send(config, smtp_password, msg, recipients)
        logger.info("Recovery notification sent for task %s.", task.name)
    except Exception as exc:
        logger.error("Failed to send recovery notification for task %s: %s", task.name, exc)


def _resolve_product_retention_days(db: Session) -> int:
    env_value = (os.getenv("STORE_MONITOR_RETENTION_DAYS") or "").strip()
    if env_value:
        try:
            parsed = int(env_value)
            if parsed >= 1:
                return parsed
        except ValueError:
            logger.warning("Invalid STORE_MONITOR_RETENTION_DAYS=%s, fallback to config.", env_value)

    config = db.query(SystemConfig).first()
    if config and isinstance(config.product_retention_days, int) and config.product_retention_days >= 1:
        return config.product_retention_days
    return DEFAULT_PRODUCT_RETENTION_DAYS


def prune_removed_products_history() -> None:
    """Delete removed products older than retention days to control DB size."""
    db = SessionLocal()
    try:
        retention_days = _resolve_product_retention_days(db)
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=retention_days)
        deleted_count = (
            db.query(ProductItem)
            .filter(ProductItem.removed_at.isnot(None))
            .filter(ProductItem.removed_at < cutoff)
            .delete(synchronize_session=False)
        )
        db.commit()
        logger.info(
            "Product history cleanup finished: removed=%d, retention_days=%d.",
            deleted_count,
            retention_days,
        )
    except Exception as exc:
        db.rollback()
        logger.error("Product history cleanup failed: %s", exc)
    finally:
        db.close()


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
    """Attempt scrape up to 1 + _TASK_RETRY_ATTEMPTS times, retrying on empty results."""
    for attempt in range(1 + _TASK_RETRY_ATTEMPTS):
        current, new_prods, removed_prods = fetch_products_for_task(db, task_id)
        if current:
            return current, new_prods, removed_prods
        if attempt < _TASK_RETRY_ATTEMPTS:
            delay = random.uniform(*_TASK_RETRY_DELAY)
            logger.warning(
                "Task %s returned 0 products (attempt %d/%d), retrying in %.0fs...",
                task_name, attempt + 1, 1 + _TASK_RETRY_ATTEMPTS, delay,
            )
            time.sleep(delay)
    return current, new_prods, removed_prods


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
        logger.error("Error executing task %s: %s", task_id, exc)
    finally:
        db.close()


def _validate_task_url(task_id: int, url: str) -> bool:
    """Validate task URL against security policy."""
    try:
        validate_monitor_target_url(url)
        return True
    except ValueError as exc:
        logger.error("Task %s blocked by URL security policy: %s", task_id, exc)
        return False


def _handle_empty_scrape_result(db, task) -> None:
    """Handle case when scrape returns no products."""
    now = datetime.datetime.now(datetime.timezone.utc)
    count = (task.consecutive_empty_count or 0) + 1
    task.consecutive_empty_count = count
    task.health_state = "alert" if count >= EMPTY_ALERT_THRESHOLD else "warning"

    logger.warning(
        "Task %s: scrape returned 0 products (consecutive=%d/%d).",
        task.name,
        count,
        EMPTY_ALERT_THRESHOLD,
    )

    if count == EMPTY_ALERT_THRESHOLD:
        task.last_health_alert_at = now
        logger.error(
            "Task %s reached %d consecutive empty scrapes. Sending health alert.",
            task.name,
            count,
        )
        db.commit()
        _send_health_alert(db, task, count)
    else:
        db.commit()


def _handle_successful_scrape(db, task, current, new_prods, removed_prods, is_first_successful_run: bool) -> None:
    """Handle successful scrape result."""
    now = datetime.datetime.now(datetime.timezone.utc)

    # Reset failure counter and notify if previously alerting
    prev_count = task.consecutive_empty_count or 0
    if prev_count >= EMPTY_ALERT_THRESHOLD:
        _send_recovery_notification(db, task, prev_count)
        task.last_recovery_at = now

    task.consecutive_empty_count = 0
    task.health_state = "healthy"
    task.last_run_at = now
    task.next_run_at = now + datetime.timedelta(hours=task.check_interval_hours)
    db.commit()

    if new_prods or removed_prods:
        logger.info(
            "Task %s: new=%d, removed=%d, queuing for digest.",
            task.name, len(new_prods), len(removed_prods),
        )
        _queue_digest_entry(task.name, new_prods, removed_prods)
    elif is_first_successful_run:
        logger.info(
            "Task %s: baseline scrape completed with %d products. Queuing initial digest.",
            task.name,
            len(current),
        )
        _queue_digest_entry(task.name, current, [])
    else:
        logger.info("Task %s: no changes detected.", task.name)


def _queue_digest_entry(task_name: str, new_products: list, removed_products: list) -> None:
    with _digest_lock:
        _pending_digest.append({
            "task_name": task_name,
            "new_products": list(new_products),
            "removed_products": list(removed_products),
        })


def _flush_digest() -> None:
    """Send one consolidated email for all accumulated changes, then clear the list."""
    with _digest_lock:
        if not _pending_digest:
            return
        entries = list(_pending_digest)
        _pending_digest.clear()

    db = SessionLocal()
    try:
        _send_consolidated_email(db, entries)
    except Exception as exc:
        logger.error("Failed to send consolidated digest: %s", exc)
    finally:
        db.close()


def _send_consolidated_email(db: Session, entries: list[dict]) -> None:
    """Render and send a single email covering all task changes."""
    # Collect recipients from ALL active tasks (union).
    all_tasks = db.query(MonitorTask).filter(MonitorTask.is_active.is_(True)).all()
    all_recipients: set[str] = set()
    for t in all_tasks:
        for r in _parse_recipients(t.recipients):
            all_recipients.add(r)
    if not all_recipients:
        logger.error("No recipients found for consolidated digest.")
        return

    config = db.query(SystemConfig).first()
    if not config or not config.sender_email:
        logger.error("SMTP not configured, cannot send digest.")
        return
    smtp_password = os.getenv("STORE_MONITOR_SMTP_PASSWORD") or decrypt_secret(config.sender_password or "")
    if not smtp_password:
        logger.error("SMTP password not configured.")
        return

    total_new = sum(len(e["new_products"]) for e in entries)
    total_removed = sum(len(e["removed_products"]) for e in entries)
    subject_parts = []
    if total_new:
        subject_parts.append(f"{total_new} 新上架")
    if total_removed:
        subject_parts.append(f"{total_removed} 已下架")
    subject = f"[Monitor] 综合报告: {', '.join(subject_parts)} (涉及 {len(entries)} 个店铺)"

    sections = [
        "<div style='font-family:sans-serif;padding:20px;'>",
        "<h2>店铺监控综合报告</h2>",
        f"<p style='color:#6b7280;'>本次报告涵盖 {len(entries)} 个店铺的变化。</p>",
    ]

    for entry in entries:
        safe_name = escape(entry["task_name"], quote=False)
        sections.append(f"<hr style='border:none;border-top:1px solid #e5e7eb;margin:24px 0'>")
        sections.append(f"<h3>{safe_name}</h3>")
        if entry["new_products"]:
            sections.append(f"<h4 style='color:#16a34a;'>新上架 ({len(entry['new_products'])})</h4>")
            sections.append(_render_product_table(entry["new_products"], "#f0fdf4", "#111111", "#4f46e5"))
        if entry["removed_products"]:
            sections.append(f"<h4 style='color:#dc2626;'>已下架 ({len(entry['removed_products'])})</h4>")
            sections.append(_render_product_table(entry["removed_products"], "#fef2f2", "#6b7280", "#6b7280"))
        if not entry["new_products"] and not entry["removed_products"]:
            sections.append("<p style='color:#6b7280;'>初始基线数据已建立。</p>")

    sections.append("</div>")

    recipients_list = sorted(all_recipients)
    msg = MIMEText("".join(sections), "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = config.sender_email
    msg["To"] = ", ".join(recipients_list)
    try:
        _smtp_send(config, smtp_password, msg, recipients_list)
        logger.info("Consolidated digest sent to %d recipients (%d entries).", len(recipients_list), len(entries))
    except Exception as exc:
        logger.error("Failed to send consolidated digest email: %s", exc)


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

        # Start the single serial-execution worker thread.
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
