import os

from fastapi.testclient import TestClient

from models import SystemConfig
from routes.settings import public_router, router as settings_router


def test_update_settings_invalid_port_redirects_with_error(app_factory):
    app = app_factory(settings_router)
    client = TestClient(app)

    response = client.post(
        "/settings",
        data={
            "smtp_server": "smtp.example.com",
            "smtp_port": "70000",
            "sender_email": "sender@example.com",
            "sender_password": "",
            "product_retention_days": "90",
            "proxy_url": "",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?error=SMTP+port+must+be+between+1+and+65535."


def test_update_settings_persists_values_and_redirects_success(app_factory, session_factory, monkeypatch):
    monkeypatch.setattr("routes.settings.encrypt_secret", lambda value: f"enc::{value}")
    monkeypatch.setattr("routes.settings.validate_proxy_url", lambda value: value.strip())

    app = app_factory(settings_router)
    client = TestClient(app)

    response = client.post(
        "/settings",
        data={
            "smtp_server": "smtp.example.com",
            "smtp_port": "465",
            "sender_email": "sender@example.com",
            "sender_password": "secret-token",
            "backup_smtp_enabled": "true",
            "backup_smtp_server": "smtp.gmail.com",
            "backup_smtp_port": "587",
            "backup_sender_email": "backup@gmail.com",
            "backup_sender_password": "backup-secret",
            "product_retention_days": "120",
            "proxy_url": "http://proxy.example.com:7890",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?success=1"

    with session_factory() as db:
        config = db.query(SystemConfig).first()

    assert config is not None
    assert config.smtp_server == "smtp.example.com"
    assert config.smtp_port == 465
    assert config.sender_email == "sender@example.com"
    assert config.sender_password == "enc::secret-token"
    assert config.backup_smtp_enabled is True
    assert config.backup_smtp_server == "smtp.gmail.com"
    assert config.backup_smtp_port == 587
    assert config.backup_sender_email == "backup@gmail.com"
    assert config.backup_sender_password == "enc::backup-secret"
    assert config.proxy_url == "http://proxy.example.com:7890"


def test_update_settings_requires_backup_password_before_enabling(app_factory, session_factory, monkeypatch):
    monkeypatch.setattr("routes.settings.encrypt_secret", lambda value: f"enc::{value}")
    app = app_factory(settings_router)
    client = TestClient(app)

    response = client.post(
        "/settings",
        data={
            "smtp_server": "smtp.example.com",
            "smtp_port": "465",
            "sender_email": "sender@example.com",
            "sender_password": "secret-token",
            "backup_smtp_enabled": "true",
            "backup_smtp_server": "smtp.gmail.com",
            "backup_smtp_port": "587",
            "backup_sender_email": "backup@gmail.com",
            "backup_sender_password": "",
            "product_retention_days": "90",
            "proxy_url": "",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == (
        "/settings?error=Backup+email+app+password+is+required+before+enabling+backup+SMTP."
    )


def test_update_settings_rejects_invalid_private_proxy(app_factory):
    os.environ.pop("MONITOR_WEB_ALLOW_PRIVATE_PROXY", None)
    app = app_factory(settings_router)
    client = TestClient(app)

    response = client.post(
        "/settings",
        data={
            "smtp_server": "smtp.example.com",
            "smtp_port": "465",
            "sender_email": "sender@example.com",
            "sender_password": "",
            "product_retention_days": "90",
            "proxy_url": "http://192.168.0.10:3128",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?error=Proxy+host+cannot+be+a+private%2Floopback+IP."


def test_update_settings_accepts_local_desktop_proxy(app_factory, session_factory):
    app = app_factory(settings_router)
    client = TestClient(app)

    response = client.post(
        "/settings",
        data={
            "smtp_server": "smtp.qq.com",
            "smtp_port": "587",
            "sender_email": "sender@example.com",
            "sender_password": "",
            "product_retention_days": "90",
            "proxy_url": "http://127.0.0.1:7897",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/settings?success=1"
    with session_factory() as db:
        config = db.query(SystemConfig).first()
        assert config.proxy_url == "http://127.0.0.1:7897"


def test_reveal_settings_returns_current_secret_without_cache(app_factory, session_factory, monkeypatch):
    monkeypatch.setattr("routes.settings.decrypt_secret", lambda value: value.replace("enc::", ""))
    with session_factory() as db:
        db.add(
            SystemConfig(
                smtp_server="smtp.qq.com",
                smtp_port=465,
                sender_email="sender@qq.com",
                sender_password="enc::current-code",
            )
        )
        db.commit()

    app = app_factory(settings_router)
    client = TestClient(app)
    response = client.post("/settings/reveal/primary")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["values"]["sender_password"] == "current-code"


def test_reveal_settings_returns_backup_defaults(app_factory):
    app = app_factory(settings_router)
    client = TestClient(app)

    response = client.post("/settings/reveal/backup")

    assert response.status_code == 200
    assert response.json()["values"]["backup_smtp_server"] == "smtp.gmail.com"
    assert response.json()["values"]["backup_smtp_port"] == 465


def test_reveal_settings_rejects_unknown_section(app_factory):
    app = app_factory(settings_router)
    client = TestClient(app)

    response = client.post("/settings/reveal/unknown")

    assert response.status_code == 422


def test_setup_marks_config_complete_and_updates_cache(app_factory, session_factory, monkeypatch):
    monkeypatch.setattr("routes.settings.encrypt_secret", lambda value: f"enc::{value}")
    monkeypatch.setattr("routes.settings.shutdown_scheduler", lambda *args, **kwargs: None)
    monkeypatch.setattr("routes.settings.init_scheduler", lambda *args, **kwargs: None)

    app = app_factory(public_router)
    client = TestClient(app)

    response = client.post(
        "/setup",
        data={
            "admin_password": "secret1",
            "smtp_server": "smtp.example.com",
            "smtp_port": "465",
            "sender_email": "sender@example.com",
            "sender_password": "smtp-secret",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert app.state.setup_complete_cache is True

    with session_factory() as db:
        config = db.query(SystemConfig).first()

    assert config is not None
    assert config.setup_complete is True
    assert config.admin_password_enc == "enc::secret1"
    assert config.sender_password == "enc::smtp-secret"
