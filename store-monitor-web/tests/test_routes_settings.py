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

    app = app_factory(settings_router)
    client = TestClient(app)

    response = client.post(
        "/settings",
        data={
            "smtp_server": "smtp.example.com",
            "smtp_port": "465",
            "sender_email": "sender@example.com",
            "sender_password": "secret-token",
            "product_retention_days": "120",
            "proxy_url": "http://127.0.0.1:7890",
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
    assert config.product_retention_days == 120
    assert config.proxy_url == "http://127.0.0.1:7890"


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
