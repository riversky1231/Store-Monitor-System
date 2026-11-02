"""Settings and setup routes."""
import os
from typing import Literal

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session

from database import get_db
from models import SystemConfig
from security import decrypt_secret, encrypt_secret, is_valid_email, require_admin_auth, validate_proxy_url
from scheduler import init_scheduler, shutdown_scheduler

from . import templates
from ._shared import _build_url

router = APIRouter(dependencies=[Depends(require_admin_auth)])
public_router = APIRouter()  # No auth — used for setup wizard.


def _settings_redirect(*, success: int | None = None, error: str | None = None) -> RedirectResponse:
    return RedirectResponse(
        url=_build_url("/settings", success=success, error=error),
        status_code=303,
    )


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, db: Session = Depends(get_db)):
    config = db.query(SystemConfig).first()
    if not config:
        config = SystemConfig()
        db.add(config)
        db.commit()
        db.refresh(config)
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "request": request,
            "config": config,
            "has_sender_password": bool(config.sender_password),
            "has_backup_sender_password": bool(config.backup_sender_password),
        },
    )


@router.post("/settings/reveal/{section}")
async def reveal_settings_section(
    section: Literal["primary", "backup", "proxy"],
    db: Session = Depends(get_db),
):
    """Reveal one configuration section only after an authenticated admin action."""
    config = db.query(SystemConfig).first()
    if section == "primary":
        values = {
            "smtp_server": (config.smtp_server if config else "") or "smtp.qq.com",
            "smtp_port": int((config.smtp_port if config else 0) or 465),
            "sender_email": (config.sender_email if config else "") or "",
            "sender_password": decrypt_secret((config.sender_password if config else "") or ""),
        }
        summary = "已显示主 SMTP 当前配置。" if config else "已显示主 SMTP 默认配置。"
    elif section == "backup":
        values = {
            "backup_smtp_server": (config.backup_smtp_server if config else "") or "smtp.gmail.com",
            "backup_smtp_port": int((config.backup_smtp_port if config else 0) or 465),
            "backup_sender_email": (config.backup_sender_email if config else "") or "",
            "backup_sender_password": decrypt_secret(
                (config.backup_sender_password if config else "") or ""
            ),
        }
        has_current = bool(
            config
            and (config.backup_smtp_server or config.backup_sender_email or config.backup_sender_password)
        )
        summary = "已显示备用 SMTP 当前配置。" if has_current else "已显示 Gmail 默认服务器配置。"
    else:
        values = {"proxy_url": (config.proxy_url if config else "") or ""}
        summary = "已显示当前代理配置。" if values["proxy_url"] else "当前默认不使用网络代理。"

    return JSONResponse(
        {"values": values, "summary": summary},
        headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
    )


@router.post("/settings")
async def update_settings(
    smtp_server: str = Form(...),
    smtp_port: int = Form(...),
    sender_email: str = Form(...),
    sender_password: str = Form(""),
    backup_smtp_enabled: bool = Form(False),
    backup_smtp_server: str = Form(""),
    backup_smtp_port: int = Form(465),
    backup_sender_email: str = Form(""),
    backup_sender_password: str = Form(""),
    proxy_url: str = Form(""),
    db: Session = Depends(get_db),
):
    config = db.query(SystemConfig).first()
    if not config:
        config = SystemConfig()
        db.add(config)

    smtp_server_value = smtp_server.strip()
    sender = sender_email.strip()
    backup_server_value = backup_smtp_server.strip()
    backup_sender = backup_sender_email.strip()
    if not smtp_server_value:
        return _settings_redirect(error="SMTP server cannot be empty.")
    if smtp_port < 1 or smtp_port > 65535:
        return _settings_redirect(error="SMTP port must be between 1 and 65535.")
    if not sender:
        return _settings_redirect(error="Sender email cannot be empty.")
    if not is_valid_email(sender):
        return _settings_redirect(error="Sender email is invalid.")
    backup_has_any_value = bool(
        backup_server_value
        or backup_sender
        or backup_sender_password.strip()
        or backup_smtp_enabled
    )
    if backup_has_any_value:
        if backup_smtp_port < 1 or backup_smtp_port > 65535:
            return _settings_redirect(error="Backup SMTP port must be between 1 and 65535.")
        if not backup_server_value:
            return _settings_redirect(error="Backup SMTP server cannot be empty.")
        if not backup_sender:
            return _settings_redirect(error="Backup sender email cannot be empty.")
        if not is_valid_email(backup_sender):
            return _settings_redirect(error="Backup sender email is invalid.")
        if backup_smtp_enabled and not backup_sender_password.strip() and not config.backup_sender_password:
            return _settings_redirect(error="Backup email app password is required before enabling backup SMTP.")
    try:
        proxy_value = validate_proxy_url(proxy_url)
    except ValueError as exc:
        return _settings_redirect(error=str(exc))

    config.smtp_server = smtp_server_value
    config.smtp_port = smtp_port
    config.sender_email = sender
    config.backup_smtp_enabled = bool(backup_smtp_enabled and backup_has_any_value)
    config.backup_smtp_server = backup_server_value
    config.backup_smtp_port = backup_smtp_port
    config.backup_sender_email = backup_sender
    config.proxy_url = proxy_value
    if sender_password.strip():
        config.sender_password = encrypt_secret(sender_password)
    if backup_sender_password.strip():
        config.backup_sender_password = encrypt_secret(backup_sender_password)

    db.commit()

    # Apply proxy changes to environment (thread-safe via validation)
    proxy = (config.proxy_url or "").strip()
    if proxy:
        try:
            validated_proxy = validate_proxy_url(proxy)
            os.environ["HTTP_PROXY"] = validated_proxy
            os.environ["HTTPS_PROXY"] = validated_proxy
        except ValueError:
            os.environ.pop("HTTP_PROXY", None)
            os.environ.pop("HTTPS_PROXY", None)
    else:
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("HTTPS_PROXY", None)

    return _settings_redirect(success=1)


# ---------------------------------------------------------------------------
# Setup wizard (no auth)
# ---------------------------------------------------------------------------

@public_router.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request, db: Session = Depends(get_db)):
    config = db.query(SystemConfig).first()
    if config and config.setup_complete:
        request.app.state.setup_complete_cache = True
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(request, "setup.html", {"request": request})


@public_router.post("/setup", response_class=HTMLResponse)
async def complete_setup(
    request: Request,
    admin_password: str = Form(...),
    smtp_server: str = Form(...),
    smtp_port: int = Form(...),
    sender_email: str = Form(...),
    sender_password: str = Form(...),
    db: Session = Depends(get_db),
):
    def _err(msg: str):
        return templates.TemplateResponse(request, "setup.html", {"request": request, "error": msg}, status_code=400)

    if len(admin_password.strip()) < 6:
        return _err("密码至少需要 6 位。")
    if not smtp_server.strip():
        return _err("SMTP 服务器不能为空。")
    if smtp_port < 1 or smtp_port > 65535:
        return _err("SMTP 端口无效。")
    sender = sender_email.strip()
    if not sender:
        return _err("发件邮箱不能为空。")
    if not is_valid_email(sender):
        return _err("发件邮箱格式无效。")
    if not sender_password.strip():
        return _err("邮箱授权码不能为空。")

    try:
        enc_admin = encrypt_secret(admin_password)
        enc_smtp = encrypt_secret(sender_password)
    except RuntimeError as exc:
        return _err(str(exc))

    config = db.query(SystemConfig).first()
    if not config:
        config = SystemConfig()
        db.add(config)

    config.admin_password_enc = enc_admin
    config.smtp_server = smtp_server.strip()
    config.smtp_port = smtp_port
    config.sender_email = sender
    config.sender_password = enc_smtp
    config.setup_complete = True
    db.commit()
    request.app.state.setup_complete_cache = True

    # Restart scheduler so it picks up new config
    shutdown_scheduler()
    init_scheduler()

    return RedirectResponse(url="/", status_code=303)
