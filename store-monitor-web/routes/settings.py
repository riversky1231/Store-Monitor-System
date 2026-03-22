"""Settings and setup routes."""
import os

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from database import get_db
from models import SystemConfig
from security import encrypt_secret, is_valid_email, require_admin_auth
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
        "settings.html",
        {
            "request": request,
            "config": config,
            "has_sender_password": bool(config.sender_password),
        },
    )


@router.post("/settings")
async def update_settings(
    smtp_server: str = Form(...),
    smtp_port: int = Form(...),
    sender_email: str = Form(...),
    sender_password: str = Form(""),
    product_retention_days: int = Form(90),
    proxy_url: str = Form(""),
    db: Session = Depends(get_db),
):
    smtp_server_value = smtp_server.strip()
    sender = sender_email.strip()
    if not smtp_server_value:
        return _settings_redirect(error="SMTP server cannot be empty.")
    if smtp_port < 1 or smtp_port > 65535:
        return _settings_redirect(error="SMTP port must be between 1 and 65535.")
    if not sender:
        return _settings_redirect(error="Sender email cannot be empty.")
    if not is_valid_email(sender):
        return _settings_redirect(error="Sender email is invalid.")
    if product_retention_days < 7 or product_retention_days > 365:
        return _settings_redirect(error="Product retention days must be between 7 and 365.")

    config = db.query(SystemConfig).first()
    if not config:
        config = SystemConfig()
        db.add(config)

    config.smtp_server = smtp_server_value
    config.smtp_port = smtp_port
    config.sender_email = sender
    config.product_retention_days = product_retention_days
    config.proxy_url = proxy_url.strip()
    if sender_password.strip():
        config.sender_password = encrypt_secret(sender_password)

    db.commit()

    proxy = config.proxy_url or ""
    if proxy:
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy
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
    return templates.TemplateResponse("setup.html", {"request": request})


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
        return templates.TemplateResponse("setup.html", {"request": request, "error": msg}, status_code=400)

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
