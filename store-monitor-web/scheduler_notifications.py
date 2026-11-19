import datetime
import json
import logging
import os
import socket
import smtplib
import ssl
import threading
import time
import re
import uuid
from email.message import Message
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape
from types import SimpleNamespace
from urllib.parse import urlparse

from sqlalchemy import or_
from sqlalchemy.orm import Session

from models import MonitorTask, ProductItem, ReportOutbox, SystemConfig
from security import decrypt_secret, encrypt_secret, is_valid_email
from smtp_diagnostics import email_domain, proxy_summary, send_smtp_once

logger = logging.getLogger(__name__)

_DIGEST_LOCK = threading.Lock()
_PENDING_DIGEST: list[dict] = []
_OUTBOX_DELIVERY_LOCK = threading.Lock()
_OUTBOX_RETRY_STATUSES = ("pending", "failed", "sending")
_OUTBOX_RETRY_LIMIT = 50
_OUTBOX_HELD_STATUS = "held_review"
_REVIEW_REMINDER_KIND = "review_reminder"
_REVIEW_REMINDER_SUFFIX = ":manual-review-reminder"
_PRIMARY_SMTP_PASSWORD_ENV = "STORE_MONITOR_SMTP_PASSWORD"
_BACKUP_SMTP_PASSWORD_ENV = "STORE_MONITOR_BACKUP_SMTP_PASSWORD"
_PUBLIC_DELIVERY_ERROR = "邮件发送失败，请查看日志后重试。"
_PROXY_INTERCEPTED_ERROR = (
    "邮件连接被本机代理（TUN/虚拟网卡模式）接管，代理节点未放行邮件端口。"
    "请在代理规则中把 smtp.qq.com 设为直连（DIRECT），或更换允许 465/587 端口的节点后重试。"
)
_SMTP_CONNECT_TIMEOUT_SECONDS = max(
    5, int(os.getenv("MONITOR_WEB_SMTP_CONNECT_TIMEOUT_SECONDS", "15") or "15")
)
_SMTP_DATA_TIMEOUT_SECONDS = max(
    _SMTP_CONNECT_TIMEOUT_SECONDS,
    int(os.getenv("MONITOR_WEB_SMTP_DATA_TIMEOUT_SECONDS", "60") or "60"),
)
_OUTBOX_RETRY_BASE_SECONDS = max(
    60, int(os.getenv("MONITOR_WEB_EMAIL_RETRY_BASE_SECONDS", "300") or "300")
)
_OUTBOX_RETRY_MAX_SECONDS = max(
    _OUTBOX_RETRY_BASE_SECONDS,
    int(os.getenv("MONITOR_WEB_EMAIL_RETRY_MAX_SECONDS", "21600") or "21600"),
)


def clean_subject_text(value: str) -> str:
    return (value or "").replace("\r", " ").replace("\n", " ").strip()


def _public_delivery_error(exc: Exception) -> str:
    """Return an actionable SMTP error without exposing credentials or payloads."""
    if isinstance(exc, smtplib.SMTPAuthenticationError):
        return "SMTP 登录失败，请检查邮箱账号、授权码和账号安全状态。"
    if isinstance(exc, smtplib.SMTPRecipientsRefused):
        return "SMTP 服务器拒绝了收件地址，请检查分组的收件邮箱。"
    if isinstance(exc, smtplib.SMTPDataError):
        code = getattr(exc, "smtp_code", "未知")
        if code == 552:
            return "SMTP 服务器因邮件过大拒绝接收（代码 552）。"
        return f"SMTP 服务器在提交邮件内容时拒绝接收（代码 {code}）。"
    if getattr(exc, "proxy_intercepted", False):
        return _PROXY_INTERCEPTED_ERROR
    if isinstance(exc, ssl.SSLError):
        return "SMTP TLS 连接被中断，请检查网络后重试。"
    if isinstance(exc, (socket.timeout, TimeoutError)):
        return "SMTP 连接超时，请检查网络或邮件服务器状态后重试。"
    if isinstance(exc, (smtplib.SMTPConnectError, smtplib.SMTPServerDisconnected)):
        return "SMTP 服务器连接中断，请稍后重试。"
    if isinstance(exc, OSError):
        return "SMTP 网络连接失败，请检查网络后重试。"
    return f"邮件发送异常：{type(exc).__name__}。请查看 monitor.log。"


def _outbox_failure_message(exc: Exception) -> str:
    """Keep stored report errors generic unless the user can act on the cause."""
    if getattr(exc, "proxy_intercepted", False):
        return _PROXY_INTERCEPTED_ERROR
    return _PUBLIC_DELIVERY_ERROR


def _mark_proxy_intercepted(exc: Exception | None) -> None:
    if exc is None:
        return
    try:
        exc.proxy_intercepted = True
    except AttributeError:
        pass


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


def _migrate_plain_smtp_secret(db: Session, config: SystemConfig, attr_name: str) -> str:
    stored_password = getattr(config, attr_name, "") or ""
    if stored_password and not stored_password.startswith("enc::"):
        try:
            setattr(config, attr_name, encrypt_secret(stored_password))
            db.commit()
            stored_password = getattr(config, attr_name, "") or ""
        except RuntimeError:
            logger.warning("SMTP password migration skipped for %s: encryption backend unavailable.", attr_name)
        except Exception as exc:
            logger.warning("SMTP password migration failed for %s: %s", attr_name, exc)
    return stored_password


def _smtp_channel(
    *,
    label: str,
    smtp_server: str,
    smtp_port: int | None,
    sender_email: str,
    smtp_password: str,
    app_proxy_configured: bool = False,
    proxy_url: str = "",
):
    return SimpleNamespace(
        label=label,
        smtp_server=(smtp_server or "").strip(),
        smtp_port=int(smtp_port or 465),
        sender_email=(sender_email or "").strip(),
        smtp_password=smtp_password,
        app_proxy_configured=bool(app_proxy_configured),
        proxy_url=(proxy_url or "").strip(),
    )


def _configured_smtp_channels(db: Session) -> list:
    config = db.query(SystemConfig).first()
    if not config or not config.sender_email:
        return []

    primary_channel = None
    stored_primary = _migrate_plain_smtp_secret(db, config, "sender_password")
    primary_password = os.getenv(_PRIMARY_SMTP_PASSWORD_ENV) or decrypt_secret(stored_primary)
    if primary_password:
        primary_channel = _smtp_channel(
            label="primary",
            smtp_server=config.smtp_server,
            smtp_port=config.smtp_port,
            sender_email=config.sender_email,
            smtp_password=primary_password,
            app_proxy_configured=bool(getattr(config, "proxy_url", None)),
            proxy_url=getattr(config, "proxy_url", "") or "",
        )

    backup_enabled = bool(getattr(config, "backup_smtp_enabled", False))
    backup_server = (getattr(config, "backup_smtp_server", "") or "").strip()
    backup_sender = (getattr(config, "backup_sender_email", "") or "").strip()
    backup_channel = None
    if backup_enabled and backup_server and backup_sender:
        stored_backup = _migrate_plain_smtp_secret(db, config, "backup_sender_password")
        backup_password = os.getenv(_BACKUP_SMTP_PASSWORD_ENV) or decrypt_secret(stored_backup)
        if backup_password:
            backup_channel = _smtp_channel(
                label="backup",
                smtp_server=backup_server,
                smtp_port=getattr(config, "backup_smtp_port", 465),
                sender_email=backup_sender,
                smtp_password=backup_password,
                app_proxy_configured=bool(getattr(config, "proxy_url", None)),
                proxy_url=getattr(config, "proxy_url", "") or "",
            )
        else:
            logger.warning("Backup SMTP is enabled but backup password is not configured.")

    # The domestic primary (QQ) can be routed directly, while Gmail depends on a
    # proxy node that permits outbound SMTP ports, so it is only a fallback.
    channels = [channel for channel in (primary_channel, backup_channel) if channel]
    logger.info(
        "SMTP failover chain prepared: channels=%s",
        ",".join(channel.label for channel in channels) or "none",
    )
    return channels


def _set_message_sender(msg: Message, sender_email: str) -> None:
    if "From" in msg:
        del msg["From"]
    msg["From"] = sender_email


def _set_message_diagnostics(msg: Message, **values) -> None:
    current = dict(getattr(msg, "_monitor_diagnostics", {}) or {})
    current.update({key: value for key, value in values.items() if value is not None})
    setattr(msg, "_monitor_diagnostics", current)


def send_with_smtp_failover(
    db: Session,
    msg: Message,
    recipients: list[str],
    *,
    retry_attempts: int = 2,
    purpose: str = "email",
) -> bool:
    channels = _configured_smtp_channels(db)
    if not channels:
        logger.error("SMTP not configured, cannot send %s.", purpose)
        return False

    trace_id = uuid.uuid4().hex[:16]
    started = time.perf_counter()
    diagnostics = dict(getattr(msg, "_monitor_diagnostics", {}) or {})
    diagnostics["trace_id"] = trace_id
    diagnostics["purpose"] = purpose
    setattr(msg, "_monitor_diagnostics", diagnostics)
    recipient_domains = sorted({email_domain(item) for item in recipients})
    proxy_env = ",".join(
        name
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY")
        if (os.getenv(name) or "").strip()
    ) or "none"
    logger.info(
        "SMTP_DIAG message_begin trace_id=%s purpose=%s message_bytes=%d recipient_count=%d "
        "recipient_domains=%s entry_count=%s new_products=%s removed_products=%s current_products=%s "
        "report_kind=%s retry_attempts=%d channels=%s app_proxy_configured=%s proxy_env=%s "
        "ipv6_supported=%s tls_runtime=%s",
        trace_id,
        purpose,
        len(msg.as_bytes()),
        len(recipients),
        ",".join(recipient_domains) or "none",
        diagnostics.get("entry_count", "unknown"),
        diagnostics.get("new_products", "unknown"),
        diagnostics.get("removed_products", "unknown"),
        diagnostics.get("current_products", "unknown"),
        diagnostics.get("report_kind", diagnostics.get("alert_kind", "generic")),
        retry_attempts,
        ",".join(channel.label for channel in channels),
        str(any(getattr(channel, "app_proxy_configured", False) for channel in channels)).lower(),
        proxy_env,
        str(bool(getattr(socket, "has_ipv6", False))).lower(),
        ssl.OPENSSL_VERSION.replace(" ", "_"),
    )

    last_exc: Exception | None = None
    intercepted_servers: list[str] = []
    for index, channel in enumerate(channels):
        _set_message_sender(msg, channel.sender_email)
        logger.info(
            "SMTP_DIAG channel_begin trace_id=%s channel=%s server=%s port=%d sender_domain=%s purpose=%s",
            trace_id,
            channel.label,
            channel.smtp_server,
            channel.smtp_port,
            email_domain(channel.sender_email),
            purpose,
        )
        try:
            smtp_send(
                channel,
                channel.smtp_password,
                msg,
                recipients,
                retry_attempts=retry_attempts,
            )
            if index:
                logger.warning(
                    "Email sent via fallback SMTP after previous channel failure: trace_id=%s channel=%s server=%s sender_domain=%s purpose=%s",
                    trace_id,
                    channel.label,
                    channel.smtp_server,
                    email_domain(channel.sender_email),
                    purpose,
                )
            else:
                logger.info(
                    "Email sent via first SMTP channel: trace_id=%s channel=%s server=%s sender_domain=%s purpose=%s",
                    trace_id,
                    channel.label,
                    channel.smtp_server,
                    email_domain(channel.sender_email),
                    purpose,
                )
            logger.info(
                "SMTP_DIAG message_result trace_id=%s result=accepted channel=%s total_elapsed_ms=%d",
                trace_id,
                channel.label,
                round((time.perf_counter() - started) * 1000),
            )
            return True
        except Exception as exc:
            last_exc = exc
            if getattr(exc, "proxy_intercepted", False):
                intercepted_servers.append(channel.smtp_server)
            logger.warning(
                "SMTP channel failed: trace_id=%s channel=%s purpose=%s type=%s reason=%s",
                trace_id,
                channel.label,
                purpose,
                type(exc).__name__,
                _public_delivery_error(exc),
            )

    if last_exc:
        if intercepted_servers:
            _mark_proxy_intercepted(last_exc)
            logger.error(
                "SMTP_DIAG proxy_intercepted trace_id=%s servers=%s hint=%s",
                trace_id,
                ",".join(intercepted_servers),
                _PROXY_INTERCEPTED_ERROR,
            )
        logger.error(
            "SMTP_DIAG message_result trace_id=%s result=failed final_exception=%s total_elapsed_ms=%d",
            trace_id,
            type(last_exc).__name__,
            round((time.perf_counter() - started) * 1000),
        )
        raise last_exc
    return False


def safe_link_for_html(link: str) -> str:
    candidate = (link or "").strip()
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "#"
    return escape(candidate, quote=True)


def load_smtp_context(
    db: Session, task: MonitorTask
) -> tuple[object, str, list[str]] | None:
    """Resolve and validate all prerequisites for sending email.

    Migrates a plaintext SMTP password to encrypted form as a side effect.
    Returns (config, smtp_password, recipients) or None if anything is missing.
    """
    recipients = parse_recipients(task.recipients)
    if not recipients:
        logger.error("No valid recipients for task %s.", task.name)
        return None

    channels = _configured_smtp_channels(db)
    if not channels:
        logger.error("SMTP sender email/password is not configured.")
        return None
    channel = channels[0]
    return channel, channel.smtp_password, recipients


def render_product_table(
    products: list,
    header_bg: str,
    text_color: str,
    link_color: str,
    max_items: int | None = 50,
) -> str:
    """Return an HTML table string; pass ``None`` to include every link."""
    display_products = products if max_items is None else products[:max_items]
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
    if max_items is not None and len(products) > max_items:
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


_ATTACHMENT_FILENAME_UNSAFE_RE = re.compile(r'[\\/:*?"<>|\r\n\t]+')
_REPORT_LOCAL_TZ = datetime.timezone(datetime.timedelta(hours=8))


def current_products_attachment_filename(report_label: str, now: datetime.datetime | None = None) -> str:
    """Build a Windows-safe attachment name for the full current-product list."""
    stamp = (now or datetime.datetime.now(datetime.timezone.utc)).astimezone(_REPORT_LOCAL_TZ)
    label = _ATTACHMENT_FILENAME_UNSAFE_RE.sub("_", (report_label or "").strip()).strip(" ._")[:60]
    prefix = f"{label}_" if label else ""
    return f"{prefix}全部商品清单_{stamp.strftime('%Y%m%d_%H%M')}.html"


def render_current_products_note(product_count: int, attachment_filename: str) -> str:
    return (
        "<p style='color:#6b7280;font-size:13px;margin-top:18px;'>"
        f"当前全部商品 <b>{product_count}</b> 个，完整链接见附件 "
        f"<b>{escape(attachment_filename, quote=False)}</b>。</p>"
    )


def render_current_products_attachment(report_title: str, store_sections: list[dict]) -> str:
    """Render a standalone HTML document listing every current product per store.

    Mail clients such as Gmail strip ``details``/``summary`` and form controls,
    so the full list cannot be collapsed inside the message body; it is shipped
    as an attachment that opens in a regular browser instead.
    """
    safe_title = escape(report_title or "监控报告", quote=False)
    parts = [
        "<!DOCTYPE html><html lang='zh-CN'><head><meta charset='utf-8'>",
        f"<title>{safe_title} - 全部商品清单</title></head>",
        "<body style='font-family:sans-serif;padding:20px;'>",
        f"<h2>{safe_title} - 全部商品清单</h2>",
    ]
    for section in store_sections:
        products = section.get("products") or []
        safe_store = escape(section.get("task_name") or "未命名任务", quote=False)
        safe_group = escape(section.get("group_name") or "未分组", quote=False)
        parts.append("<hr style='border:none;border-top:1px solid #e5e7eb;margin:24px 0'>")
        parts.append(f"<h3 data-section='store'>{safe_store} ({len(products)})</h3>")
        parts.append(f"<p style='color:#6b7280;font-size:13px;margin-top:-8px;'>分组：{safe_group}</p>")
        parts.append(render_product_table(products, "#eff6ff", "#111111", "#2563eb", max_items=None))
    parts.append("</body></html>")
    return "".join(parts)


def smtp_send(
    config: SystemConfig,
    smtp_password: str,
    msg: Message,
    recipients: list[str],
    retry_attempts: int = 2,
) -> None:
    last_exc: Exception | None = None
    diagnostics = dict(getattr(msg, "_monitor_diagnostics", {}) or {})
    trace_id = str(diagnostics.get("trace_id") or uuid.uuid4().hex[:16])
    channel = str(getattr(config, "label", "direct") or "direct")
    logger.info(
        "SMTP_DIAG transport_begin trace_id=%s channel=%s server=%s port=%d sender_domain=%s "
        "recipient_count=%d message_bytes=%d retry_attempts=%d proxy=%s",
        trace_id,
        channel,
        config.smtp_server,
        config.smtp_port,
        email_domain(config.sender_email),
        len(recipients),
        len(msg.as_bytes()),
        retry_attempts,
        proxy_summary(getattr(config, "proxy_url", "")),
    )

    configured_port = int(config.smtp_port)
    ports = [587, 465] if configured_port in (465, 587) else [configured_port]
    configured_proxy = (getattr(config, "proxy_url", "") or "").strip()
    routes = [("proxy", configured_proxy), ("direct", "")] if configured_proxy else [("direct", "")]
    candidate_count = len(ports) * len(routes)
    diagnostic_attempt = 0
    route_intercepted = False

    for retry_round in range(1 + retry_attempts):
        for port in ports:
            for route_name, proxy_url in routes:
                diagnostic_attempt += 1
                logger.info(
                    "SMTP_DIAG candidate_begin trace_id=%s channel=%s round=%d/%d candidate=%d/%d "
                    "route=%s port=%d proxy=%s",
                    trace_id,
                    channel,
                    retry_round + 1,
                    1 + retry_attempts,
                    ((diagnostic_attempt - 1) % candidate_count) + 1,
                    candidate_count,
                    route_name,
                    port,
                    proxy_summary(proxy_url),
                )
                try:
                    send_smtp_once(
                        config,
                        smtp_password,
                        msg,
                        recipients,
                        port=port,
                        trace_id=trace_id,
                        channel=channel,
                        attempt=diagnostic_attempt,
                        timeout=_SMTP_CONNECT_TIMEOUT_SECONDS,
                        proxy_url=proxy_url,
                        data_timeout=_SMTP_DATA_TIMEOUT_SECONDS,
                    )
                    logger.info(
                        "SMTP_DIAG candidate_result trace_id=%s channel=%s route=%s port=%d result=accepted",
                        trace_id,
                        channel,
                        route_name,
                        port,
                    )
                    return
                except Exception as exc:
                    last_exc = exc
                    route_intercepted = route_intercepted or bool(getattr(exc, "proxy_intercepted", False))
                    logger.warning(
                        "SMTP_DIAG candidate_result trace_id=%s channel=%s route=%s port=%d "
                        "result=failed exception=%s reason=%s",
                        trace_id,
                        channel,
                        route_name,
                        port,
                        type(exc).__name__,
                        _public_delivery_error(exc),
                    )
        if retry_round < retry_attempts:
            wait = 5 * (retry_round + 1)
            logger.warning(
                "SMTP send round failed: trace_id=%s channel=%s round=%d/%d retry_in_seconds=%d reason=%s",
                trace_id,
                channel,
                retry_round + 1,
                1 + retry_attempts,
                wait,
                _public_delivery_error(last_exc) if last_exc else "unknown",
            )
            time.sleep(wait)

    if last_exc:
        if route_intercepted:
            _mark_proxy_intercepted(last_exc)
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
    _set_message_diagnostics(
        msg,
        entry_count=1,
        new_products=len(new_products),
        removed_products=len(removed_products),
    )
    try:
        send_with_smtp_failover(
            db,
            msg,
            recipients,
            retry_attempts=retry_attempts,
            purpose=f"task report for {task.name}",
        )
        logger.info("Email sent for task %s to %d recipient(s).", task.name, len(recipients))
    except Exception as exc:
        logger.error("Failed to send email for task %s: %s", task.name, _public_delivery_error(exc))


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
    _set_message_diagnostics(msg, entry_count=1, alert_kind="health")
    try:
        send_with_smtp_failover(
            db,
            msg,
            recipients,
            retry_attempts=retry_attempts,
            purpose=f"health alert for {task.name}",
        )
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
    _set_message_diagnostics(msg, entry_count=1, alert_kind="recovery")
    try:
        send_with_smtp_failover(
            db,
            msg,
            recipients,
            retry_attempts=retry_attempts,
            purpose=f"recovery notice for {task.name}",
        )
        logger.info("Recovery notification sent for task %s.", task.name)
    except Exception as exc:
        logger.error("Failed to send recovery notification for task %s: %s", task.name, exc)


def queue_digest_entry(
    task: MonitorTask,
    new_products: list,
    removed_products: list,
    is_baseline: bool = False,
    baseline_count: int = 0,
    digest_scope: dict | None = None,
    started_at = None,
    current_products: list | None = None,
    status: str = "success",
    status_message: str = "",
) -> None:
    group_name = task.group.name if getattr(task, "group", None) else "未分组"
    scope = dict(digest_scope or {})
    if not scope:
        scope = {
            "key": f"group:{task.category_id or 'ungrouped'}",
            "label": group_name,
            "kind": "group",
        }
    logger.debug(
        "Queuing digest entry: scope=%s, task=%s, new=%d, removed=%d, baseline=%s",
        scope.get("key"),
        task.name,
        len(new_products),
        len(removed_products),
        is_baseline,
    )
    with _DIGEST_LOCK:
        _PENDING_DIGEST.append(
            {
                "task_id": task.id,
                "task_name": task.name,
                "group_id": task.category_id,
                "group_name": group_name,
                "scope_key": scope.get("key") or f"group:{task.category_id or 'ungrouped'}",
                "scope_label": scope.get("label") or group_name,
                "scope_kind": scope.get("kind") or "group",
                "new_products": list(new_products),
                "removed_products": list(removed_products),
                "current_products": list(current_products or []),
                "is_baseline": is_baseline,
                "baseline_count": baseline_count,
                "status": (status or "success").strip() or "success",
                "status_message": (status_message or "").strip(),
                "started_at": started_at.isoformat() if getattr(started_at, "isoformat", None) else "",
                "task_url": (getattr(task, "url", "") or "").strip(),
                "recipients": parse_recipients(getattr(task, "recipients", "") or ""),
            }
        )


def _serialize_digest_entries(entries: list[dict]) -> str:
    return json.dumps(
        entries,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )


def _deserialize_digest_entries(row: ReportOutbox) -> list[dict]:
    payload = json.loads(row.payload_json)
    if not isinstance(payload, list) or not payload:
        raise ValueError("Report outbox payload must be a non-empty list.")
    if not all(isinstance(entry, dict) for entry in payload):
        raise ValueError("Report outbox payload contains an invalid entry.")
    return payload


def _merge_digest_entries(
    existing: list[dict],
    replacements: list[dict],
    pending_task_ids: set[int],
) -> list[dict]:
    """Update one batch without dropping other stores or undoing manual decisions."""
    replacement_by_task = {
        int(entry["task_id"]): entry
        for entry in replacements
        if entry.get("task_id") is not None
    }
    merged: list[dict] = []
    seen_task_ids: set[int] = set()
    for entry in existing:
        task_id = entry.get("task_id")
        normalized_id = int(task_id) if task_id is not None else None
        if normalized_id is not None and normalized_id in replacement_by_task:
            replacement = replacement_by_task[normalized_id]
            if (
                entry.get("status") == "confirmed"
                and replacement.get("status") == "failed"
                and normalized_id not in pending_task_ids
            ):
                # A queued retry can arrive after the user has already handled it.
                merged.append(entry)
            else:
                merged.append(replacement)
            seen_task_ids.add(normalized_id)
        else:
            merged.append(entry)
    for entry in replacements:
        task_id = entry.get("task_id")
        normalized_id = int(task_id) if task_id is not None else None
        if normalized_id is None or normalized_id not in seen_task_ids:
            merged.append(entry)
            if normalized_id is not None:
                seen_task_ids.add(normalized_id)
    return merged


def _upsert_outbox_report(
    db: Session,
    scope_entries: list[dict],
    *,
    pending_task_ids: set[int],
) -> ReportOutbox:
    scope_key = (scope_entries[0].get("scope_key") or "").strip()
    if not scope_key:
        raise ValueError("Report scope key is required for durable delivery.")

    hold_for_review = bool(pending_task_ids)
    row = db.query(ReportOutbox).filter(ReportOutbox.scope_key == scope_key).first()
    if row is None:
        row = ReportOutbox(
            scope_key=scope_key,
            scope_label=(scope_entries[0].get("scope_label") or "").strip(),
            scope_kind=(scope_entries[0].get("scope_kind") or "group").strip() or "group",
            payload_json=_serialize_digest_entries(scope_entries),
            status=_OUTBOX_HELD_STATUS if hold_for_review else "pending",
            attempt_count=0,
        )
        db.add(row)
    elif row.status != "sent":
        row.scope_label = (scope_entries[0].get("scope_label") or row.scope_label or "").strip()
        row.scope_kind = (scope_entries[0].get("scope_kind") or row.scope_kind or "group").strip()
        # A review can release the report while retry entries are still queued.
        # Merge into the original snapshot even after a delivery attempt failed.
        if row.payload_json != "[]":
            existing_entries = _deserialize_digest_entries(row)
            scope_entries = _merge_digest_entries(existing_entries, scope_entries, pending_task_ids)
        row.payload_json = _serialize_digest_entries(scope_entries)
        row.status = _OUTBOX_HELD_STATUS if hold_for_review else "pending"
        row.last_error = None
        row.next_attempt_at = None

    db.commit()
    db.refresh(row)
    return row


def _pending_review_tasks(db: Session, scope_key: str) -> list[MonitorTask]:
    return (
        db.query(MonitorTask)
        .filter(
            MonitorTask.manual_review_pending.is_(True),
            MonitorTask.is_active.is_(True),
            MonitorTask.manual_review_scope_key == scope_key,
        )
        .order_by(MonitorTask.id.asc())
        .all()
    )


def _active_products_for_task(db: Session, task_id: int) -> list[dict]:
    products = (
        db.query(ProductItem)
        .filter(
            ProductItem.task_id == task_id,
            ProductItem.removed_at.is_(None),
        )
        .order_by(ProductItem.id.asc())
        .all()
    )
    return [
        {"name": (product.name or "").strip(), "link": (product.product_link or "").strip()}
        for product in products
        if (product.product_link or "").strip()
    ]


def _review_reminder_scope_key(scope_key: str) -> str:
    return f"{scope_key}{_REVIEW_REMINDER_SUFFIX}"


def _build_review_reminder_entries(
    report_row: ReportOutbox,
    report_entries: list[dict],
    pending_tasks: list[MonitorTask],
) -> list[dict]:
    entries_by_task = {
        int(entry["task_id"]): entry
        for entry in report_entries
        if entry.get("task_id") is not None
    }
    reminder_entries: list[dict] = []
    for task in pending_tasks:
        source = entries_by_task.get(task.id, {})
        started_at = task.manual_review_started_at
        reminder_entries.append(
            {
                "task_id": task.id,
                "task_name": (source.get("task_name") or task.name or f"任务 {task.id}").strip(),
                "group_id": source.get("group_id", task.category_id),
                "group_name": source.get("group_name")
                or (task.group.name if getattr(task, "group", None) else "未分组"),
                "scope_key": report_row.scope_key,
                "scope_label": report_row.scope_label,
                "scope_kind": report_row.scope_kind,
                "status": "manual_review",
                "status_message": (task.last_failure_reason or source.get("status_message") or "需要人工确认").strip(),
                "manual_review_kind": (task.manual_review_kind or "failure").strip(),
                "review_started_at": started_at.isoformat()
                if getattr(started_at, "isoformat", None)
                else "",
                "task_url": (source.get("task_url") or task.url or "").strip(),
                "recipients": source.get("recipients") or parse_recipients(task.recipients or ""),
            }
        )
    return reminder_entries


def _ensure_review_reminder(
    db: Session,
    report_row: ReportOutbox,
    report_entries: list[dict],
    pending_tasks: list[MonitorTask],
) -> ReportOutbox:
    reminder_entries = _build_review_reminder_entries(report_row, report_entries, pending_tasks)
    payload_json = _serialize_digest_entries(reminder_entries)
    reminder_key = _review_reminder_scope_key(report_row.scope_key)
    reminder = db.query(ReportOutbox).filter(ReportOutbox.scope_key == reminder_key).first()
    if reminder is None:
        reminder = ReportOutbox(
            scope_key=reminder_key,
            scope_label=report_row.scope_label,
            scope_kind=_REVIEW_REMINDER_KIND,
            payload_json=payload_json,
            status="pending",
            attempt_count=0,
        )
        db.add(reminder)
    elif reminder.payload_json != payload_json:
        keep_sent_reminder = False
        if reminder.status == "sent":
            try:
                previous_entries = _deserialize_digest_entries(reminder)
                previous_by_task = {
                    int(entry["task_id"]): entry
                    for entry in previous_entries
                    if entry.get("task_id") is not None
                }
                # Resolving part of a group only shrinks the pending set and
                # must not generate another reminder. A retry failure changes
                # review_started_at (or, for legacy rows, its reason) and is a
                # genuinely new action that should be mailed once.
                keep_sent_reminder = all(
                    int(entry["task_id"]) in previous_by_task
                    and (
                        entry.get("review_started_at")
                        == previous_by_task[int(entry["task_id"])].get("review_started_at")
                    )
                    and (
                        bool(entry.get("review_started_at"))
                        or entry.get("status_message")
                        == previous_by_task[int(entry["task_id"])].get("status_message")
                    )
                    for entry in reminder_entries
                    if entry.get("task_id") is not None
                )
            except Exception:
                keep_sent_reminder = False
        if not keep_sent_reminder:
            # A manual retry can fail for a new reason.  A changed reminder is
            # a new actionable state and should be delivered once even if the
            # first reminder for this batch was already sent.
            reminder.scope_label = report_row.scope_label
            reminder.payload_json = payload_json
            reminder.status = "pending"
            reminder.last_error = None
            reminder.next_attempt_at = None
            reminder.sent_at = None
    db.commit()
    db.refresh(reminder)
    return reminder


def _cancel_unsent_review_reminder(db: Session, scope_key: str) -> None:
    reminder = (
        db.query(ReportOutbox)
        .filter(ReportOutbox.scope_key == _review_reminder_scope_key(scope_key))
        .first()
    )
    if reminder is not None and reminder.status != "sent":
        reminder.status = "cancelled"
        reminder.payload_json = "[]"
        reminder.last_error = None
        db.commit()


def _mark_resolved_report_entries(
    db: Session,
    entries: list[dict],
    pending_task_ids: set[int],
    *,
    target_task_id: int | None = None,
    resolution_message: str = "人工确认已完成",
) -> list[dict]:
    updated: list[dict] = []
    for source in entries:
        entry = dict(source)
        task_id_raw = entry.get("task_id")
        task_id = int(task_id_raw) if task_id_raw is not None else None
        should_resolve = (
            task_id is not None
            and task_id not in pending_task_ids
            and entry.get("status") not in {"success", "confirmed"}
            and (target_task_id is None or task_id == target_task_id)
        )
        if should_resolve:
            entry["status"] = "confirmed"
            entry["status_message"] = resolution_message
            entry["current_products"] = _active_products_for_task(db, task_id)
        updated.append(entry)
    return updated


def _reconcile_held_reports_locked(
    db: Session,
    excluded_scope_keys: set[str] | None = None,
) -> None:
    query = db.query(ReportOutbox).filter(ReportOutbox.status == _OUTBOX_HELD_STATUS)
    if excluded_scope_keys:
        query = query.filter(~ReportOutbox.scope_key.in_(excluded_scope_keys))
    held_rows = query.order_by(
        ReportOutbox.created_at.asc(),
        ReportOutbox.id.asc(),
    ).limit(_OUTBOX_RETRY_LIMIT).all()
    for row in held_rows:
        try:
            entries = _deserialize_digest_entries(row)
        except Exception as exc:
            _mark_outbox_failed(db, row, f"暂存报告数据损坏：{exc}")
            continue
        pending_tasks = _pending_review_tasks(db, row.scope_key)
        if pending_tasks:
            _ensure_review_reminder(db, row, entries, pending_tasks)
            continue

        row.payload_json = _serialize_digest_entries(
            _mark_resolved_report_entries(db, entries, set())
        )
        row.status = "pending"
        row.last_error = None
        row.next_attempt_at = None
        _cancel_unsent_review_reminder(db, row.scope_key)
        db.commit()


def _outbox_retry_delay_seconds(attempt_count: int) -> int:
    exponent = max(0, min(8, int(attempt_count or 1) - 1))
    return min(_OUTBOX_RETRY_MAX_SECONDS, _OUTBOX_RETRY_BASE_SECONDS * (3 ** exponent))


def _mark_outbox_failed(db: Session, row: ReportOutbox, message: str = _PUBLIC_DELIVERY_ERROR) -> None:
    delay_seconds = _outbox_retry_delay_seconds(int(row.attempt_count or 1))
    row.status = "failed"
    row.last_error = (message or _PUBLIC_DELIVERY_ERROR)[:2000]
    row.next_attempt_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=delay_seconds
    )
    db.commit()
    logger.warning(
        "Report retry scheduled: report_id=%d attempt_count=%d retry_in_seconds=%d next_attempt_at=%s",
        row.id,
        int(row.attempt_count or 0),
        delay_seconds,
        row.next_attempt_at.isoformat(),
    )


def _legacy_report_task_ids(db: Session, row: ReportOutbox) -> list[int]:
    """Find current tasks for an old report whose snapshot was not retained."""
    parts = (row.scope_key or "").split(":")
    if parts and parts[0] == "task" and len(parts) > 1 and parts[1].isdigit():
        task_id = int(parts[1])
        return [task_id] if db.query(MonitorTask.id).filter(MonitorTask.id == task_id).first() else []
    if parts and parts[0] == "group" and len(parts) > 1 and parts[1].isdigit():
        return [
            task.id
            for task in db.query(MonitorTask)
            .filter(MonitorTask.category_id == int(parts[1]))
            .order_by(MonitorTask.sort_order.asc(), MonitorTask.id.asc())
            .all()
        ]
    return [
        task.id
        for task in db.query(MonitorTask)
        .order_by(MonitorTask.category_id.asc(), MonitorTask.sort_order.asc(), MonitorTask.id.asc())
        .all()
    ]


def _rebuild_legacy_report_payload(db: Session, row: ReportOutbox) -> list[dict]:
    """Recreate a resendable current snapshot for legacy rows with payload '[]'."""
    task_ids = _legacy_report_task_ids(db, row)
    tasks = db.query(MonitorTask).filter(MonitorTask.id.in_(task_ids)).order_by(MonitorTask.id.asc()).all() if task_ids else []
    entries = []
    for task in tasks:
        group_name = task.group.name if getattr(task, "group", None) else "未分组"
        current_products = _active_products_for_task(db, task.id)
        entries.append({
            "task_id": task.id,
            "task_name": task.name or f"任务 {task.id}",
            "group_id": task.category_id,
            "group_name": group_name,
            "scope_key": row.scope_key,
            "scope_label": row.scope_label or group_name,
            "scope_kind": row.scope_kind or "group",
            "new_products": [],
            "removed_products": [],
            "current_products": current_products,
            "is_baseline": False,
            "baseline_count": len(current_products),
            "status": "success",
            "status_message": "根据当前商品数据重建的报告",
            "started_at": "",
            "task_url": task.url or "",
            "recipients": parse_recipients(task.recipients or ""),
        })
    if entries:
        row.payload_json = _serialize_digest_entries(entries)
        row.last_error = None
        db.commit()
        logger.info("Rebuilt legacy email report payload: report_id=%d tasks=%d.", row.id, len(entries))
    return entries


def _deliver_outbox_row(db: Session, row: ReportOutbox, retry_attempts: int) -> bool:
    if row.status == "sent":
        return True
    if row.status not in _OUTBOX_RETRY_STATUSES:
        return False

    now = datetime.datetime.now(datetime.timezone.utc)
    logger.info(
        "Report delivery started: report_id=%d scope=%s kind=%s status=%s previous_attempts=%d",
        row.id,
        row.scope_label or "未命名报告",
        row.scope_kind,
        row.status,
        int(row.attempt_count or 0),
    )
    row.status = "sending"
    row.attempt_count = int(row.attempt_count or 0) + 1
    row.last_attempt_at = now
    row.last_error = None
    db.commit()

    try:
        entries = _deserialize_digest_entries(row)
    except Exception as exc:
        logger.error(
            "Invalid durable report payload: report_id=%d scope=%s type=%s reason=%s",
            row.id,
            row.scope_key,
            type(exc).__name__,
            _public_delivery_error(exc),
        )
        _mark_outbox_failed(db, row)
        return False

    try:
        if row.scope_kind == _REVIEW_REMINDER_KIND:
            sent = send_manual_review_reminder(db, entries, retry_attempts=retry_attempts)
        else:
            sent = send_consolidated_email(db, entries, retry_attempts=retry_attempts)
    except Exception as exc:
        logger.error(
            "Durable report delivery crashed: report_id=%d scope=%s type=%s reason=%s",
            row.id,
            row.scope_key,
            type(exc).__name__,
            _public_delivery_error(exc),
        )
        _mark_outbox_failed(db, row, _outbox_failure_message(exc))
        return False

    if sent:
        row.status = "sent"
        row.sent_at = datetime.datetime.now(datetime.timezone.utc)
        row.last_error = None
        row.next_attempt_at = None
        db.commit()
        logger.info(
            "Durable report delivered: report_id=%d scope=%s attempts=%d.",
            row.id,
            row.scope_key,
            row.attempt_count,
        )
        return True

    _mark_outbox_failed(db, row, _PUBLIC_DELIVERY_ERROR)
    logger.error(
        "Report delivery failed: report_id=%d scope=%s attempts=%d error=%s",
        row.id,
        row.scope_label or "未命名报告",
        row.attempt_count,
        row.last_error or "未知错误",
    )
    return False


def flush_digest(
    session_factory,
    retry_attempts: int = 2,
    blocked_scope_keys: set[str] | None = None,
    only_scope_keys: set[str] | None = None,
) -> None:
    """Send completed scope reports and retain entries whose delivery fails."""
    blocked = {
        (item or "").strip()
        for item in (blocked_scope_keys or set())
        if (item or "").strip()
    }
    selected = {
        (item or "").strip()
        for item in (only_scope_keys or set())
        if (item or "").strip()
    }
    with _DIGEST_LOCK:
        if not _PENDING_DIGEST:
            logger.debug("No pending digest entries to send.")
            return
        entries_to_flush: list[dict] = []
        entries_to_keep: list[dict] = []
        for entry in _PENDING_DIGEST:
            scope_key = (entry.get("scope_key") or "").strip()
            if selected and scope_key not in selected:
                entries_to_keep.append(entry)
            elif scope_key and scope_key in blocked:
                entries_to_keep.append(entry)
            else:
                entries_to_flush.append(entry)
        _PENDING_DIGEST.clear()
        _PENDING_DIGEST.extend(entries_to_keep)

    if not entries_to_flush:
        logger.debug("No report entries matched the requested completion scope.")
        return

    logger.debug("Flushing digest with %d entries.", len(entries_to_flush))
    entries_by_scope: dict[str, list[dict]] = {}
    for entry in entries_to_flush:
        entries_by_scope.setdefault(entry.get("scope_key") or "group:ungrouped", []).append(entry)

    unpersisted_entries: list[dict] = []
    with _OUTBOX_DELIVERY_LOCK:
        db = session_factory()
        try:
            for scope_entries in entries_by_scope.values():
                try:
                    scope_key = (scope_entries[0].get("scope_key") or "").strip()
                    pending_tasks = _pending_review_tasks(db, scope_key)
                    row = _upsert_outbox_report(
                        db,
                        scope_entries,
                        pending_task_ids={task.id for task in pending_tasks},
                    )
                except Exception as exc:
                    db.rollback()
                    logger.error(
                        "Failed to persist report scope %s before delivery: %s",
                        scope_entries[0].get("scope_key"),
                        exc,
                    )
                    unpersisted_entries.extend(scope_entries)
                    continue
                if pending_tasks:
                    report_entries = _deserialize_digest_entries(row)
                    reminder = _ensure_review_reminder(
                        db,
                        row,
                        report_entries,
                        pending_tasks,
                    )
                    _deliver_outbox_row(db, reminder, retry_attempts)
                    logger.info(
                        "Held complete report %s for %d manual review task(s).",
                        row.scope_key,
                        len(pending_tasks),
                    )
                else:
                    _cancel_unsent_review_reminder(db, row.scope_key)
                    _deliver_outbox_row(db, row, retry_attempts)
        finally:
            db.close()

    if unpersisted_entries:
        with _DIGEST_LOCK:
            _PENDING_DIGEST[0:0] = unpersisted_entries
        logger.error(
            "Retained %d report entries in memory because outbox persistence failed.",
            len(unpersisted_entries),
        )


def retry_pending_reports(
    session_factory,
    retry_attempts: int = 2,
    exclude_scope_keys: set[str] | None = None,
    wait_for_lock: bool = True,
    respect_schedule: bool = False,
) -> dict[str, int]:
    """Retry durable unsent reports in creation order without duplicating active scopes."""
    excluded = {
        (item or "").strip()
        for item in (exclude_scope_keys or set())
        if (item or "").strip()
    }
    result = {"attempted": 0, "sent": 0, "failed": 0}
    acquired = _OUTBOX_DELIVERY_LOCK.acquire(blocking=wait_for_lock)
    if not acquired:
        return {**result, "busy": 1}
    try:
        db = session_factory()
        try:
            _reconcile_held_reports_locked(db, excluded)
            query = db.query(ReportOutbox).filter(ReportOutbox.status.in_(_OUTBOX_RETRY_STATUSES))
            if respect_schedule:
                now = datetime.datetime.now(datetime.timezone.utc)
                query = query.filter(
                    or_(
                        ReportOutbox.status == "sending",
                        ReportOutbox.next_attempt_at.is_(None),
                        ReportOutbox.next_attempt_at <= now,
                    )
                )
            if excluded:
                query = query.filter(~ReportOutbox.scope_key.in_(excluded))
            rows = query.order_by(ReportOutbox.created_at.asc(), ReportOutbox.id.asc()).limit(
                _OUTBOX_RETRY_LIMIT
            ).all()
            for row in rows:
                result["attempted"] += 1
                if _deliver_outbox_row(db, row, retry_attempts):
                    result["sent"] += 1
                else:
                    result["failed"] += 1
        finally:
            db.close()
    finally:
        _OUTBOX_DELIVERY_LOCK.release()

    if result["attempted"]:
        logger.info(
            "Durable report retry finished: attempted=%d, sent=%d, failed=%d.",
            result["attempted"],
            result["sent"],
            result["failed"],
        )
    return result


def retry_report_by_id(
    session_factory,
    report_id: int,
    retry_attempts: int = 2,
    wait_for_lock: bool = False,
) -> dict[str, int]:
    """Resend one durable report, including a previously accepted report."""
    result = {"attempted": 0, "sent": 0, "failed": 0, "busy": 0, "unavailable": 0}
    logger.info("Report resend started: report_id=%d", report_id)
    acquired = _OUTBOX_DELIVERY_LOCK.acquire(blocking=wait_for_lock)
    if not acquired:
        result["busy"] = 1
        return result
    try:
        db = session_factory()
        try:
            row = db.query(ReportOutbox).filter(ReportOutbox.id == report_id).first()
            if row is None or row.status in {_OUTBOX_HELD_STATUS, "cancelled"}:
                logger.warning(
                    "Report resend unavailable: report_id=%d reason=%s",
                    report_id,
                    "not_found" if row is None else row.status,
                )
                result["unavailable"] = 1
                return result
            if row.status == "sending":
                logger.warning("Report resend busy: report_id=%d", report_id)
                result["busy"] = 1
                return result
            if not (row.payload_json or "").strip() or row.payload_json == "[]":
                if not _rebuild_legacy_report_payload(db, row):
                    logger.warning(
                        "Legacy report cannot be rebuilt because its tasks no longer exist: report_id=%d",
                        report_id,
                    )
                    result["unavailable"] = 1
                    return result

            row.status = "pending"
            row.last_error = None
            row.next_attempt_at = None
            db.commit()
            result["attempted"] = 1
            if _deliver_outbox_row(db, row, retry_attempts):
                result["sent"] = 1
            else:
                result["failed"] = 1
            logger.info(
                "Report resend finished: report_id=%d sent=%d failed=%d attempts=%d",
                report_id,
                result["sent"],
                result["failed"],
                int(row.attempt_count or 0),
            )
            return result
        finally:
            db.close()
    finally:
        _OUTBOX_DELIVERY_LOCK.release()


def resolve_held_report_task(
    session_factory,
    scope_key: str | None,
    task_id: int,
    *,
    resolution_message: str = "人工确认已完成",
    retry_attempts: int = 2,
) -> dict[str, object]:
    """Update one held task and release its full report when the scope is clear."""
    normalized_scope = (scope_key or "").strip()
    result: dict[str, object] = {
        "found": False,
        "released": False,
        "delivered": False,
        "remaining": 0,
    }
    if not normalized_scope:
        return result

    with _OUTBOX_DELIVERY_LOCK:
        db = session_factory()
        try:
            row = (
                db.query(ReportOutbox)
                .filter(
                    ReportOutbox.scope_key == normalized_scope,
                    ReportOutbox.status == _OUTBOX_HELD_STATUS,
                )
                .first()
            )
            if row is None:
                return result

            result["found"] = True
            entries = _deserialize_digest_entries(row)
            pending_tasks = _pending_review_tasks(db, normalized_scope)
            pending_task_ids = {task.id for task in pending_tasks}
            row.payload_json = _serialize_digest_entries(
                _mark_resolved_report_entries(
                    db,
                    entries,
                    pending_task_ids,
                    target_task_id=task_id,
                    resolution_message=resolution_message,
                )
            )
            result["remaining"] = len(pending_tasks)
            if pending_tasks:
                db.commit()
                return result

            # Resolve any other entries whose DB state was committed before a
            # prior process exit, then release one complete group report.
            entries = json.loads(row.payload_json)
            row.payload_json = _serialize_digest_entries(
                _mark_resolved_report_entries(db, entries, set())
            )
            row.status = "pending"
            row.last_error = None
            row.next_attempt_at = None
            _cancel_unsent_review_reminder(db, normalized_scope)
            db.commit()
            result["released"] = True
            result["delivered"] = _deliver_outbox_row(db, row, retry_attempts)
            return result
        finally:
            db.close()


def get_report_delivery_status(db: Session, page: int = 1, page_size: int = 10) -> dict:
    page = max(1, int(page or 1))
    page_size = max(1, min(50, int(page_size or 10)))
    pending_count = db.query(ReportOutbox).filter(ReportOutbox.status == "pending").count()
    sending_count = db.query(ReportOutbox).filter(ReportOutbox.status == "sending").count()
    failed_count = db.query(ReportOutbox).filter(ReportOutbox.status == "failed").count()
    held_count = db.query(ReportOutbox).filter(ReportOutbox.status == _OUTBOX_HELD_STATUS).count()
    latest_problem = (
        db.query(ReportOutbox)
        .filter(ReportOutbox.status.in_(("failed", "sending", "pending")))
        .order_by(ReportOutbox.updated_at.desc(), ReportOutbox.id.desc())
        .first()
    )
    latest_sent = (
        db.query(ReportOutbox)
        .filter(ReportOutbox.status == "sent")
        .order_by(ReportOutbox.sent_at.desc(), ReportOutbox.id.desc())
        .first()
    )
    latest_held = (
        db.query(ReportOutbox)
        .filter(ReportOutbox.status == _OUTBOX_HELD_STATUS)
        .order_by(ReportOutbox.updated_at.desc(), ReportOutbox.id.desc())
        .first()
    )
    report_query = (
        db.query(ReportOutbox)
        .filter(ReportOutbox.status != "cancelled")
        .order_by(ReportOutbox.updated_at.desc(), ReportOutbox.id.desc())
    )
    total_reports = report_query.count()
    total_pages = max(1, (total_reports + page_size - 1) // page_size)
    page = min(page, total_pages)
    recent_rows = report_query.offset((page - 1) * page_size).limit(page_size).all()
    reports = []
    for row in recent_rows:
        has_payload = bool((row.payload_json or "").strip() not in {"", "[]"})
        reports.append(
            {
                "id": row.id,
                "scope_label": row.scope_label or "未命名报告",
                "scope_kind": row.scope_kind,
                "status": row.status,
                "attempt_count": int(row.attempt_count or 0),
                "last_error": _PUBLIC_DELIVERY_ERROR if row.status == "failed" else "",
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "last_attempt_at": row.last_attempt_at.isoformat() if row.last_attempt_at else None,
                "sent_at": row.sent_at.isoformat() if row.sent_at else None,
                "can_retry": row.status not in {_OUTBOX_HELD_STATUS, "sending"} and (has_payload or row.status == "sent"),
            }
        )
    return {
        "pending_count": pending_count,
        "sending_count": sending_count,
        "failed_count": failed_count,
        "held_count": held_count,
        "unsent_count": pending_count + sending_count + failed_count,
        "latest_error": _PUBLIC_DELIVERY_ERROR if latest_problem else "",
        "latest_scope": (latest_problem.scope_label or "") if latest_problem else "",
        "latest_attempt_count": int(latest_problem.attempt_count or 0) if latest_problem else 0,
        "latest_sent_at": latest_sent.sent_at.isoformat() if latest_sent and latest_sent.sent_at else None,
        "latest_held_scope": (latest_held.scope_label or "") if latest_held else "",
        "page": page,
        "page_size": page_size,
        "total_reports": total_reports,
        "total_pages": total_pages,
        "reports": reports,
    }


def _resolve_recipients_for_task_ids(db: Session, task_ids: list[int]) -> tuple[list[str], dict[int, MonitorTask]]:
    tasks_by_id: dict[int, MonitorTask] = {}
    if task_ids:
        rows = db.query(MonitorTask).filter(MonitorTask.id.in_(task_ids)).all()
        tasks_by_id = {task.id: task for task in rows}

    recipients: set[str] = set()
    for task in tasks_by_id.values():
        for recipient in parse_recipients(task.recipients):
            recipients.add(recipient)

    return sorted(recipients), tasks_by_id


def _resolve_digest_recipients(
    db: Session,
    entries: list[dict],
) -> tuple[list[str], dict[int, MonitorTask]]:
    task_ids = sorted({int(entry["task_id"]) for entry in entries if entry.get("task_id")})
    recipients_list, tasks_by_id = _resolve_recipients_for_task_ids(db, task_ids)
    persisted_recipients: set[str] = set(recipients_list)
    for entry in entries:
        raw_recipients = entry.get("recipients") or []
        if isinstance(raw_recipients, str):
            raw_recipients = parse_recipients(raw_recipients)
        for recipient in raw_recipients:
            candidate = str(recipient or "").strip()
            if is_valid_email(candidate):
                persisted_recipients.add(candidate)
    return sorted(persisted_recipients), tasks_by_id


def _load_digest_smtp_context(db: Session) -> tuple[object, str] | None:
    channels = _configured_smtp_channels(db)
    if not channels:
        logger.error("SMTP not configured, cannot send report email.")
        return None
    channel = channels[0]
    return channel, channel.smtp_password


def send_manual_review_reminder(
    db: Session,
    entries: list[dict],
    retry_attempts: int = 2,
) -> bool:
    """Send an action-only reminder containing no normal-store report data."""
    recipients, _tasks_by_id = _resolve_digest_recipients(db, entries)
    if not recipients:
        logger.error("No recipients found for manual-review reminder.")
        return False
    smtp_context = _load_digest_smtp_context(db)
    if not smtp_context:
        return False
    config, smtp_password = smtp_context

    scope_label = clean_subject_text(entries[0].get("scope_label") or "未分组") or "未分组"
    subject = f"[Monitor] {scope_label}：{len(entries)} 个店铺需要人工确认"
    sections = [
        "<div style='font-family:sans-serif;padding:20px;'>",
        f"<h2 style='color:#d97706;'>{escape(scope_label, quote=False)} 需要人工确认</h2>",
        "<p>本轮完整报告已暂存，以下店铺需要先在系统中完成“设为正常”或“重试”。</p>",
        "<p style='color:#6b7280;'>所有待确认店铺处理完成后，系统才会发送本分组的完整报告。</p>",
        "<ul style='padding-left:22px;'>",
    ]
    for entry in entries:
        safe_name = escape((entry.get("task_name") or "未命名店铺").strip(), quote=False)
        safe_reason = escape((entry.get("status_message") or "需要人工确认").strip(), quote=False)
        sections.append(
            f"<li style='margin:12px 0;'><b>{safe_name}</b>"
            f"<br><span style='color:#92400e;'>{safe_reason}</span></li>"
        )
    sections.extend(["</ul>", "<p>请打开 Store Monitor 的待人工确认列表进行处理。</p>", "</div>"])

    msg = MIMEText("".join(sections), "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = config.sender_email
    msg["To"] = ", ".join(recipients)
    _set_message_diagnostics(msg, entry_count=len(entries), report_kind="manual_review")
    try:
        send_with_smtp_failover(
            db,
            msg,
            recipients,
            retry_attempts=retry_attempts,
            purpose="manual-review reminder",
        )
        logger.info(
            "Manual-review reminder sent to %d recipient(s) for %d task(s).",
            len(recipients),
            len(entries),
        )
        return True
    except Exception as exc:
        logger.error("Failed to send manual-review reminder: %s", exc)
        raise


def send_consolidated_email(
    db: Session,
    entries: list[dict],
    retry_attempts: int = 2,
) -> bool:
    """Render and send a single email covering all task changes in the same scope."""
    recipients_list, tasks_by_id = _resolve_digest_recipients(db, entries)
    if not recipients_list:
        logger.error("No recipients found for consolidated digest.")
        return False

    logger.debug(
        "Digest recipient summary: count=%d domains=%s",
        len(recipients_list),
        ",".join(sorted({email_domain(item) for item in recipients_list})) or "none",
    )

    smtp_context = _load_digest_smtp_context(db)
    if not smtp_context:
        return False
    config, smtp_password = smtp_context

    logger.debug(
        "SMTP configured: server=%s, port=%s, sender=%s",
        config.smtp_server,
        config.smtp_port,
        config.sender_email,
    )

    total_new = sum(len(entry["new_products"]) for entry in entries)
    total_removed = sum(len(entry["removed_products"]) for entry in entries)
    total_baseline = sum(1 for entry in entries if entry.get("is_baseline"))
    total_failed = sum(
        1
        for entry in entries
        if entry.get("status") not in {"success", "confirmed", "cancelled"}
    )
    total_confirmed = sum(1 for entry in entries if entry.get("status") == "confirmed")
    scope_label = entries[0].get("scope_label") or "未分组"
    scope_kind = entries[0].get("scope_kind") or "group"
    if scope_kind == "all":
        report_title = "全部任务综合报告"
    elif scope_kind == "group":
        report_title = f"{scope_label} 分组报告"
    elif scope_kind == "task":
        report_title = f"{scope_label} 运行报告"
    else:
        report_title = f"{scope_label} 监控报告"

    subject_parts = []
    if total_baseline:
        subject_parts.append(f"{total_baseline} 个店铺初始化完成")
    if total_new:
        subject_parts.append(f"{total_new} 新上架")
    if total_removed:
        subject_parts.append(f"{total_removed} 已下架")
    if total_failed:
        subject_parts.append(f"{total_failed} 个任务异常")
    if total_confirmed:
        subject_parts.append(f"{total_confirmed} 个任务已人工确认")
    subject_suffix = ", ".join(subject_parts) if subject_parts else "运行完成，无变化"
    subject = f"[Monitor] {report_title}: {subject_suffix}"

    sections = [
        "<div style='font-family:sans-serif;padding:20px;'>",
        f"<h2>{escape(report_title, quote=False)}</h2>",
        f"<p style='color:#6b7280;'>本次报告涵盖 {len(entries)} 个店铺的运行结果。</p>",
    ]

    attachment_filename = current_products_attachment_filename(scope_label)
    attachment_sections: list[dict] = []
    total_current_products = 0
    for entry in sorted(
        entries,
        key=lambda item: (
            item.get("started_at") or "",
            item.get("group_name") or "",
            item.get("task_name") or "",
        ),
    ):
        safe_name = escape(entry["task_name"], quote=False)
        safe_group = escape(entry.get("group_name") or "未分组", quote=False)
        task = tasks_by_id.get(entry.get("task_id"))
        status = entry.get("status") or "success"
        status_message = escape(entry.get("status_message") or "", quote=False)
        current_products = entry.get("current_products") or []
        total_current_products += len(current_products)
        sections.append("<hr style='border:none;border-top:1px solid #e5e7eb;margin:24px 0'>")
        sections.append(f"<h3>{safe_name}</h3>")
        if scope_kind == "all":
            sections.append(f"<p style='color:#6b7280;font-size:13px;margin-top:-8px;'>分组：{safe_group}</p>")
        else:
            task_url = (task.url if task and task.url else entry.get("task_url") or "").strip()
        if scope_kind != "all" and task_url:
            sections.append(
                f"<p style='color:#6b7280;font-size:13px;margin-top:-8px;'>"
                f"<a href='{safe_link_for_html(task_url)}' style='color:#2563eb;'>查看店铺</a></p>"
            )

        if status == "confirmed":
            sections.append(
                f"<p style='color:#d97706;font-size:14px;'><b>人工确认已完成</b>"
                f"{('：' + status_message) if status_message else ''}</p>"
            )
        elif status != "success":
            status_label = "已取消" if status == "cancelled" else "运行失败"
            sections.append(
                f"<p style='color:#dc2626;font-size:16px;'><b>{status_label}</b>"
                f"{('：' + status_message) if status_message else ''}</p>"
            )
            continue

        sections.append(
            f"<p style='color:#374151;font-size:14px;'>"
            f"运行成功；当前商品 <b>{len(current_products)}</b> 个，"
            f"新上架 <b>{len(entry['new_products'])}</b> 个，"
            f"已下架 <b>{len(entry['removed_products'])}</b> 个。</p>"
        )

        if entry.get("is_baseline"):
            sections.append(
                "<p style='color:#16a34a;font-size:14px;'>✓ 首次运行已完成商品基线初始化。</p>"
            )

        if entry["new_products"]:
            sections.append(f"<h4 style='color:#16a34a;'>新上架 ({len(entry['new_products'])})</h4>")
            sections.append(render_product_table(entry["new_products"], "#f0fdf4", "#111111", "#4f46e5", max_items=None))
        if entry["removed_products"]:
            sections.append(f"<h4 style='color:#dc2626;'>已下架 ({len(entry['removed_products'])})</h4>")
            sections.append(render_product_table(entry["removed_products"], "#fef2f2", "#6b7280", "#6b7280", max_items=None))
        if current_products:
            sections.append(render_current_products_note(len(current_products), attachment_filename))
            attachment_sections.append({
                "task_name": entry["task_name"],
                "group_name": entry.get("group_name") or "未分组",
                "products": current_products,
            })
        else:
            sections.append("<p style='color:#6b7280;'>本次运行未返回可展示的商品链接。</p>")

    sections.append("</div>")

    body = MIMEText("".join(sections), "html", "utf-8")
    if attachment_sections:
        msg = MIMEMultipart("mixed")
        msg.attach(body)
        attachment = MIMEText(
            render_current_products_attachment(report_title, attachment_sections),
            "html",
            "utf-8",
        )
        attachment.add_header(
            "Content-Disposition",
            "attachment",
            filename=("utf-8", "", attachment_filename),
        )
        msg.attach(attachment)
    else:
        msg = body
    msg["Subject"] = subject
    msg["From"] = config.sender_email
    msg["To"] = ", ".join(recipients_list)
    _set_message_diagnostics(
        msg,
        entry_count=len(entries),
        new_products=total_new,
        removed_products=total_removed,
        current_products=total_current_products,
        report_kind="consolidated",
    )
    try:
        send_with_smtp_failover(
            db,
            msg,
            recipients_list,
            retry_attempts=retry_attempts,
            purpose="consolidated digest",
        )
        logger.info("Consolidated digest sent to %d recipients (%d entries).", len(recipients_list), len(entries))
        return True
    except Exception as exc:
        logger.error("Failed to send consolidated digest email: %s", _public_delivery_error(exc))
        raise


def _format_status_timestamp(value) -> str:
    if not value:
        return "—"
    try:
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone(datetime.timedelta(hours=8)))
        return value.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)


def _render_status_task_table(title: str, tasks: list[dict], empty_text: str) -> str:
    if not tasks:
        return (
            f"<div style='margin-top:18px;'>"
            f"<h3>{escape(title, quote=False)}</h3>"
            f"<p style='color:#6b7280;'>{escape(empty_text, quote=False)}</p>"
            f"</div>"
        )

    rows = []
    for item in tasks:
        updated_text = _format_status_timestamp(item.get("updated_at"))
        rows.append(
            "<tr>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;'>{escape(item.get('task_name') or '', quote=False)}</td>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;color:#6b7280;'>{escape(item.get('group_name') or '未分组', quote=False)}</td>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;'>{escape(item.get('status_text') or '', quote=False)}</td>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;color:#6b7280;'>{escape(item.get('reason') or '—', quote=False)}</td>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;color:#6b7280;'>{escape(updated_text, quote=False)}</td>"
            "</tr>"
        )

    return (
        f"<div style='margin-top:18px;'>"
        f"<h3>{escape(title, quote=False)}</h3>"
        f"<table style='width:100%;border-collapse:collapse;margin-top:10px;'>"
        f"<thead><tr style='background:#f8fafc;text-align:left;'>"
        "<th style='padding:10px;border:1px solid #e5e7eb;'>任务</th>"
        "<th style='padding:10px;border:1px solid #e5e7eb;'>分组</th>"
        "<th style='padding:10px;border:1px solid #e5e7eb;'>状态</th>"
        "<th style='padding:10px;border:1px solid #e5e7eb;'>说明</th>"
        "<th style='padding:10px;border:1px solid #e5e7eb;'>最近时间</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
        "</div>"
    )


def _render_daily_new_products(products: list[dict]) -> str:
    if not products:
        return (
            "<div style='margin-top:18px;'>"
            "<h3>今日新上架商品</h3>"
            "<p style='color:#6b7280;'>今日暂未发现新品。</p>"
            "</div>"
        )

    rows = []
    for item in products[:100]:
        safe_name = escape(item.get("name") or "", quote=False)
        safe_task = escape(item.get("task_name") or "未命名任务", quote=False)
        safe_group = escape(item.get("group_name") or "未分组", quote=False)
        safe_time = escape(item.get("discovered_text") or "—", quote=False)
        safe_href = safe_link_for_html(item.get("link") or "")
        rows.append(
            "<tr>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;color:#111827;'>{safe_name}</td>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;color:#374151;'>{safe_task}</td>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;color:#6b7280;'>{safe_group}</td>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;color:#6b7280;'>{safe_time}</td>"
            f"<td style='padding:10px;border:1px solid #e5e7eb;'>"
            f"<a href='{safe_href}' style='color:#2563eb;'>查看商品</a></td>"
            "</tr>"
        )

    if len(products) > 100:
        rows.append(
            "<tr>"
            f"<td colspan='5' style='padding:10px;border:1px solid #e5e7eb;color:#6b7280;text-align:center;'>"
            f"还有 {len(products) - 100} 个商品未显示...</td>"
            "</tr>"
        )

    return (
        "<div style='margin-top:18px;'>"
        f"<h3>今日新上架商品 ({len(products)})</h3>"
        "<table style='width:100%;border-collapse:collapse;margin-top:10px;'>"
        "<thead><tr style='background:#f0fdf4;text-align:left;'>"
        "<th style='padding:10px;border:1px solid #e5e7eb;'>商品名称</th>"
        "<th style='padding:10px;border:1px solid #e5e7eb;'>任务</th>"
        "<th style='padding:10px;border:1px solid #e5e7eb;'>分组</th>"
        "<th style='padding:10px;border:1px solid #e5e7eb;'>发现时间</th>"
        "<th style='padding:10px;border:1px solid #e5e7eb;'>链接</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
        "</div>"
    )


def send_daily_status_email(
    db: Session,
    report: dict,
    retry_attempts: int = 2,
) -> bool:
    recipients_set: set[str] = set()
    active_tasks = db.query(MonitorTask).filter(MonitorTask.is_active.is_(True)).all()
    for task in active_tasks:
        for recipient in parse_recipients(task.recipients):
            recipients_set.add(recipient)
    recipients_list = sorted(recipients_set)
    if not recipients_list:
        logger.error("No recipients found for daily status email.")
        return False

    smtp_context = _load_digest_smtp_context(db)
    if not smtp_context:
        return False
    config, smtp_password = smtp_context

    generated_at = report.get("generated_at")
    generated_text = _format_status_timestamp(generated_at)
    abnormal_tasks = report.get("abnormal_tasks", [])
    failed_tasks = report.get("failed_tasks", [])
    new_products = report.get("new_products", [])
    report_date_text = report.get("report_date_text") or (
        generated_at.strftime("%Y-%m-%d") if generated_at else ""
    )

    subject = (
        f"[Monitor] 每日上新报告 {report_date_text}: "
        f"今日上新 {len(new_products)} 个, {len(abnormal_tasks)} 个异常, {len(failed_tasks)} 个失败"
    ).strip()
    sections = [
        "<div style='font-family:sans-serif;padding:20px;'>",
        "<h2>每日上新与监控状态报告</h2>",
        f"<p style='color:#6b7280;'>生成时间：{escape(generated_text, quote=False)}</p>",
        (
            "<div style='display:flex;gap:12px;flex-wrap:wrap;margin:18px 0;'>"
            f"<div style='padding:12px 16px;border-radius:10px;background:#eff6ff;min-width:150px;'>活跃任务<br><b>{int(report.get('active_task_count', 0))}</b></div>"
            f"<div style='padding:12px 16px;border-radius:10px;background:#dcfce7;min-width:150px;'>今日上新<br><b>{len(new_products)}</b></div>"
            f"<div style='padding:12px 16px;border-radius:10px;background:#fef3c7;min-width:150px;'>异常任务<br><b>{len(abnormal_tasks)}</b></div>"
            f"<div style='padding:12px 16px;border-radius:10px;background:#fee2e2;min-width:150px;'>失败任务<br><b>{len(failed_tasks)}</b></div>"
            f"<div style='padding:12px 16px;border-radius:10px;background:#ecfccb;min-width:150px;'>待人工确认任务<br><b>{int(report.get('retry_pending_count', 0))}</b></div>"
            "</div>"
        ),
        (
            f"<p style='color:#374151;'>当前队列：运行中 {escape(report.get('running_task_name') or '无', quote=False)}，"
            f"等待 {int(report.get('waiting_count', 0))} 个任务。</p>"
        ),
    ]
    sections.append(_render_daily_new_products(new_products))
    sections.append(_render_status_task_table("抓取异常列表", abnormal_tasks, "今日没有异常任务。"))
    if failed_tasks:
        sections.append(
            "<div style='margin-top:18px;'>"
            "<h3>失败任务提示</h3>"
            f"<p style='color:#6b7280;'>当前共有 <b>{len(failed_tasks)}</b> 个失败任务。"
            "失败任务不会写入邮件详情，避免误报和错误数据影响后续抓取，请在后台失败列表中查看并手动处理。</p>"
            "</div>"
        )
    else:
        sections.append(
            "<div style='margin-top:18px;'>"
            "<h3>失败任务提示</h3>"
            "<p style='color:#6b7280;'>今日没有失败任务。</p>"
            "</div>"
        )
    sections.append("</div>")

    msg = MIMEText("".join(sections), "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = config.sender_email
    msg["To"] = ", ".join(recipients_list)
    _set_message_diagnostics(
        msg,
        entry_count=int(report.get("active_task_count", 0)),
        new_products=len(new_products),
        report_kind="daily_status",
    )
    try:
        send_with_smtp_failover(
            db,
            msg,
            recipients_list,
            retry_attempts=retry_attempts,
            purpose="daily status email",
        )
        logger.info(
            "Daily status email sent to %d recipients (new=%d, abnormal=%d, failed=%d).",
            len(recipients_list),
            len(new_products),
            len(abnormal_tasks),
            len(failed_tasks),
        )
        return True
    except Exception as exc:
        logger.error("Failed to send daily status email: %s", _public_delivery_error(exc))
        return False
