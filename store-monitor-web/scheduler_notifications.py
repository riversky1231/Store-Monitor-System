import logging
import os
import smtplib
import threading
import time
from email.mime.text import MIMEText
from html import escape
from urllib.parse import urlparse

from sqlalchemy.orm import Session

from models import MonitorTask, SystemConfig
from security import decrypt_secret, encrypt_secret, is_valid_email

logger = logging.getLogger(__name__)

_DIGEST_LOCK = threading.Lock()
_PENDING_DIGEST: list[dict] = []


def clean_subject_text(value: str) -> str:
    return (value or "").replace("\r", " ").replace("\n", " ").strip()


def parse_recipients(raw_value: str) -> list[str]:
    recipients = []
    for item in (raw_value or "").split(","):
        if "\r" in item or "\n" in item:
            continue
        email = item.strip()
        if not email:
            continue
        if is_valid_email(email):
            recipients.append(email)
    return list(dict.fromkeys(recipients))


def safe_link_for_html(link: str) -> str:
    candidate = (link or "").strip()
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "#"
    return escape(candidate, quote=True)


def load_smtp_context(
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

    recipients = parse_recipients(task.recipients)
    if not recipients:
        logger.error("No valid recipients for task %s.", task.name)
        return None

    return config, smtp_password, recipients


def render_product_table(
    products: list,
    header_bg: str,
    text_color: str,
    link_color: str,
    max_items: int = 50,
) -> str:
    """Return an HTML table string for a list of products (limited to max_items)."""
    display_products = products[:max_items]
    rows = []
    for product in display_products:
        safe_name = escape((product.get("name") or "").strip(), quote=False)
        safe_href = safe_link_for_html(product.get("link") or "")
        rows.append(
            f"<tr>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;color:{text_color};'>{safe_name}</td>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;'>"
            f"<a href='{safe_href}' style='color:{link_color};'>查看商品</a></td></tr>"
        )
    if len(products) > max_items:
        remaining = len(products) - max_items
        rows.append(
            f"<tr><td colspan='2' style='padding:10px;border:1px solid #e5e7eb;color:#6b7280;text-align:center;'>"
            f"还有 {remaining} 个商品未显示...</td></tr>"
        )
    return (
        f"<table style='width:100%;border-collapse:collapse;margin-bottom:20px;'>"
        f"<thead><tr style='background:{header_bg};text-align:left;'>"
        f"<th style='padding:10px;border:1px solid #e5e7eb;'>商品名称</th>"
        f"<th style='padding:10px;border:1px solid #e5e7eb;'>链接</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def smtp_send(
    config: SystemConfig,
    smtp_password: str,
    msg: MIMEText,
    recipients: list[str],
    retry_attempts: int = 2,
) -> None:
    import ssl

    last_exc: Exception | None = None

    logger.debug(
        "SMTP config: server=%s, port=%d, sender=%s, recipients=%s",
        config.smtp_server,
        config.smtp_port,
        config.sender_email,
        recipients,
    )

    for attempt in range(1 + retry_attempts):
        try:
            context = ssl.create_default_context()

            if config.smtp_port == 465:
                logger.debug("Using SMTP_SSL for port %d", config.smtp_port)
                with smtplib.SMTP_SSL(
                    config.smtp_server,
                    config.smtp_port,
                    timeout=120,
                    context=context,
                ) as server:
                    server.set_debuglevel(0)
                    logger.debug("SSL connected, logging in as %s...", config.sender_email)
                    server.login(config.sender_email, smtp_password)
                    logger.debug("Login successful, sending message...")
                    server.send_message(msg, to_addrs=recipients)
            elif config.smtp_port == 587:
                logger.debug("Using SMTP+STARTTLS for port %d", config.smtp_port)
                with smtplib.SMTP(config.smtp_server, config.smtp_port, timeout=120) as server:
                    server.set_debuglevel(0)
                    logger.debug("Connected, sending EHLO...")
                    server.ehlo()
                    logger.debug("EHLO done, starting TLS...")
                    server.starttls(context=context)
                    logger.debug("TLS started, sending EHLO again...")
                    server.ehlo()
                    logger.debug("Logging in as %s...", config.sender_email)
                    server.login(config.sender_email, smtp_password)
                    logger.debug("Login successful, sending message...")
                    server.send_message(msg, to_addrs=recipients)
            else:
                logger.debug("Using plain SMTP for port %d", config.smtp_port)
                with smtplib.SMTP(config.smtp_server, config.smtp_port, timeout=60) as server:
                    server.set_debuglevel(0)
                    server.ehlo()
                    if server.has_extn("STARTTLS"):
                        server.starttls(context=context)
                        server.ehlo()
                    server.login(config.sender_email, smtp_password)
                    server.send_message(msg, to_addrs=recipients)

            logger.debug("Email sent successfully!")
            return
        except ssl.SSLError as exc:
            last_exc = exc
            logger.error("SMTP SSL error: %s", exc)
            if config.smtp_port == 465 and attempt == 0:
                logger.debug("SSL error on port 465, trying port 587 with STARTTLS...")
                try:
                    ctx587 = ssl.create_default_context()
                    with smtplib.SMTP(config.smtp_server, 587, timeout=60) as server:
                        server.ehlo()
                        server.starttls(context=ctx587)
                        server.ehlo()
                        server.login(config.sender_email, smtp_password)
                        server.send_message(msg, to_addrs=recipients)
                    logger.debug("Email sent successfully via port 587!")
                    return
                except Exception as exc_587:
                    logger.debug("Port 587 also failed: %s", exc_587)
            if attempt < retry_attempts:
                wait = 5 * (attempt + 1)
                logger.warning(
                    "SMTP send failed (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1,
                    1 + retry_attempts,
                    wait,
                    exc,
                )
                time.sleep(wait)
        except Exception as exc:
            last_exc = exc
            logger.error("SMTP error details: %s: %s", type(exc).__name__, exc)
            if attempt < retry_attempts:
                wait = 5 * (attempt + 1)
                logger.warning(
                    "SMTP send failed (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1,
                    1 + retry_attempts,
                    wait,
                    exc,
                )
                time.sleep(wait)

    if last_exc:
        raise last_exc


def send_email(
    db: Session,
    task: MonitorTask,
    new_products: list,
    removed_products: list | None = None,
    retry_attempts: int = 2,
) -> None:
    removed_products = removed_products or []
    ctx = load_smtp_context(db, task)
    if not ctx:
        return
    config, smtp_password, recipients = ctx

    task_name = clean_subject_text(task.name) or f"Task-{task.id}"
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
        sections.append(render_product_table(new_products, "#f0fdf4", "#111111", "#4f46e5"))
    if removed_products:
        sections.append(f"<h3 style='color:#dc2626;'>已下架商品 ({len(removed_products)})</h3>")
        sections.append(render_product_table(removed_products, "#fef2f2", "#6b7280", "#6b7280"))
    sections.append("</div>")

    msg = MIMEText("".join(sections), "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = config.sender_email
    msg["To"] = ", ".join(recipients)
    try:
        smtp_send(config, smtp_password, msg, recipients, retry_attempts=retry_attempts)
        logger.info("Email sent for task %s to %d recipient(s).", task.name, len(recipients))
    except Exception as exc:
        logger.error("Failed to send email for task %s: %s", task.name, exc)


def send_health_alert(
    db: Session,
    task: MonitorTask,
    consecutive_count: int,
    retry_attempts: int = 2,
) -> None:
    ctx = load_smtp_context(db, task)
    if not ctx:
        logger.error("Cannot send health alert: SMTP not configured.")
        return
    config, smtp_password, recipients = ctx

    task_name = clean_subject_text(task.name) or f"Task-{task.id}"
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
        smtp_send(config, smtp_password, msg, recipients, retry_attempts=retry_attempts)
        logger.warning("Health alert sent for task %s after %d empty scrapes.", task.name, consecutive_count)
    except Exception as exc:
        logger.error("Failed to send health alert for task %s: %s", task.name, exc)


def send_recovery_notification(
    db: Session,
    task: MonitorTask,
    prev_count: int,
    retry_attempts: int = 2,
) -> None:
    ctx = load_smtp_context(db, task)
    if not ctx:
        return
    config, smtp_password, recipients = ctx

    task_name = clean_subject_text(task.name) or f"Task-{task.id}"
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
        smtp_send(config, smtp_password, msg, recipients, retry_attempts=retry_attempts)
        logger.info("Recovery notification sent for task %s.", task.name)
    except Exception as exc:
        logger.error("Failed to send recovery notification for task %s: %s", task.name, exc)


def queue_digest_entry(
    task_name: str,
    new_products: list,
    removed_products: list,
    is_baseline: bool = False,
    baseline_count: int = 0,
) -> None:
    logger.debug(
        "Queuing digest entry: task=%s, new=%d, removed=%d, baseline=%s",
        task_name,
        len(new_products),
        len(removed_products),
        is_baseline,
    )
    with _DIGEST_LOCK:
        _PENDING_DIGEST.append(
            {
                "task_name": task_name,
                "new_products": list(new_products),
                "removed_products": list(removed_products),
                "is_baseline": is_baseline,
                "baseline_count": baseline_count,
            }
        )


def flush_digest(session_factory, retry_attempts: int = 2) -> None:
    """Send one consolidated email for all accumulated changes, then clear the list."""
    with _DIGEST_LOCK:
        if not _PENDING_DIGEST:
            logger.debug("No pending digest entries to send.")
            return
        entries = list(_PENDING_DIGEST)
        _PENDING_DIGEST.clear()

    logger.debug("Flushing digest with %d entries.", len(entries))

    db = session_factory()
    try:
        send_consolidated_email(db, entries, retry_attempts=retry_attempts)
    except Exception as exc:
        logger.error("Failed to send consolidated digest: %s", exc)
    finally:
        db.close()


def send_consolidated_email(
    db: Session,
    entries: list[dict],
    retry_attempts: int = 2,
) -> None:
    """Render and send a single email covering all task changes."""
    all_tasks = db.query(MonitorTask).filter(MonitorTask.is_active.is_(True)).all()
    all_recipients: set[str] = set()
    for task in all_tasks:
        for recipient in parse_recipients(task.recipients):
            all_recipients.add(recipient)
    if not all_recipients:
        logger.error("No recipients found for consolidated digest.")
        return

    logger.debug("Recipients: %s", all_recipients)

    config = db.query(SystemConfig).first()
    if not config or not config.sender_email:
        logger.error("SMTP not configured, cannot send digest.")
        return
    smtp_password = os.getenv("STORE_MONITOR_SMTP_PASSWORD") or decrypt_secret(config.sender_password or "")
    if not smtp_password:
        logger.warning("SMTP password not configured.")
        return

    logger.debug(
        "SMTP configured: server=%s, port=%s, sender=%s",
        config.smtp_server,
        config.smtp_port,
        config.sender_email,
    )

    total_new = sum(len(entry["new_products"]) for entry in entries)
    total_removed = sum(len(entry["removed_products"]) for entry in entries)
    total_baseline = sum(1 for entry in entries if entry.get("is_baseline"))

    subject_parts = []
    if total_baseline:
        subject_parts.append(f"{total_baseline} 个店铺初始化完成")
    if total_new:
        subject_parts.append(f"{total_new} 新上架")
    if total_removed:
        subject_parts.append(f"{total_removed} 已下架")
    subject = f"[Monitor] 综合报告: {', '.join(subject_parts)}"

    sections = [
        "<div style='font-family:sans-serif;padding:20px;'>",
        "<h2>店铺监控综合报告</h2>",
        f"<p style='color:#6b7280;'>本次报告涵盖 {len(entries)} 个店铺的变化。</p>",
    ]

    for entry in entries:
        safe_name = escape(entry["task_name"], quote=False)
        sections.append("<hr style='border:none;border-top:1px solid #e5e7eb;margin:24px 0'>")
        sections.append(f"<h3>{safe_name}</h3>")

        if entry.get("is_baseline"):
            baseline_count = entry.get("baseline_count", 0)
            sections.append(
                f"<p style='color:#16a34a;font-size:16px;'>"
                f"✓ 店铺初始化成功，共抓取到 <b>{baseline_count}</b> 个商品</p>"
            )
            sections.append(
                "<p style='color:#6b7280;font-size:14px;'>"
                "后续抓取如有新上架或下架商品，将会在邮件中详细列出。</p>"
            )
            continue

        if entry["new_products"]:
            sections.append(f"<h4 style='color:#16a34a;'>新上架 ({len(entry['new_products'])})</h4>")
            sections.append(render_product_table(entry["new_products"], "#f0fdf4", "#111111", "#4f46e5"))
        if entry["removed_products"]:
            sections.append(f"<h4 style='color:#dc2626;'>已下架 ({len(entry['removed_products'])})</h4>")
            sections.append(render_product_table(entry["removed_products"], "#fef2f2", "#6b7280", "#6b7280"))

    sections.append("</div>")

    recipients_list = sorted(all_recipients)
    msg = MIMEText("".join(sections), "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = config.sender_email
    msg["To"] = ", ".join(recipients_list)
    try:
        smtp_send(config, smtp_password, msg, recipients_list, retry_attempts=retry_attempts)
        logger.info("Consolidated digest sent to %d recipients (%d entries).", len(recipients_list), len(entries))
    except Exception as exc:
        logger.error("Failed to send consolidated digest email: %s", exc)
