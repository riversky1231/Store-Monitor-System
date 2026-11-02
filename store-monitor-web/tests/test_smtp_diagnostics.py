import logging
import socket
import smtplib
from email.mime.text import MIMEText
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import ssl

import scheduler_notifications
from smtp_diagnostics import is_proxy_fake_ip, send_smtp_once
from scheduler_notifications import (
    _PROXY_INTERCEPTED_ERROR,
    _outbox_failure_message,
    _public_delivery_error,
    _set_message_diagnostics,
    send_with_smtp_failover,
    smtp_send,
)


def _config():
    return SimpleNamespace(
        smtp_server="smtp.example.com",
        smtp_port=465,
        sender_email="sender@example.com",
    )


def _message():
    msg = MIMEText("diagnostic body", "plain", "utf-8")
    msg["Subject"] = "diagnostic"
    msg["From"] = "sender@example.com"
    msg["To"] = "recipient@example.net"
    return msg


def test_dns_failure_is_attributed_without_logging_secrets_or_addresses(caplog):
    caplog.set_level(logging.INFO)
    with patch(
        "smtp_diagnostics.socket.getaddrinfo",
        side_effect=socket.gaierror(11001, "lookup failed"),
    ):
        with pytest.raises(socket.gaierror):
            send_smtp_once(
                _config(),
                "super-secret-password",
                _message(),
                ["recipient@example.net"],
                port=465,
                trace_id="trace-dns",
                channel="backup",
                attempt=1,
                timeout=1,
            )

    log_text = caplog.text
    assert "trace_id=trace-dns" in log_text
    assert "stage=dns_lookup result=failed" in log_text
    assert "exception=gaierror" in log_text
    assert "super-secret-password" not in log_text
    assert "recipient@example.net" not in log_text
    assert "sender@example.com" not in log_text


def test_post_accept_quit_disconnect_is_logged_without_retrying(caplog):
    caplog.set_level(logging.INFO)

    class FakeSMTPSSL:
        esmtp_features = {"auth": "PLAIN"}

        def __init__(self, *args, **kwargs):
            pass

        def set_debuglevel(self, _level):
            pass

        def ehlo(self):
            return 250, b"ok"

        def login(self, _sender, _password):
            return 235, b"authenticated"

        def send_message(self, _msg, to_addrs):
            assert to_addrs == ["recipient@example.net"]
            return {}

        def quit(self):
            raise smtplib.SMTPServerDisconnected("connection closed after DATA")

        def close(self):
            pass

    with patch("smtp_diagnostics.socket.getaddrinfo", return_value=[(socket.AF_INET, 1, 6, "", ())]), patch(
        "smtp_diagnostics.DiagnosticSMTPSSL", FakeSMTPSSL
    ):
        send_smtp_once(
            _config(),
            "secret",
            _message(),
            ["recipient@example.net"],
            port=465,
            trace_id="trace-accepted",
            channel="primary",
            attempt=1,
            timeout=1,
        )

    assert "stage=message_data result=success" in caplog.text
    assert "quit_result trace_id=trace-accepted" in caplog.text
    assert "post_accept=true" in caplog.text


def test_message_diagnostics_include_counts_and_domains_without_full_addresses(caplog):
    caplog.set_level(logging.INFO)
    msg = _message()
    _set_message_diagnostics(
        msg,
        entry_count=7,
        new_products=3,
        removed_products=1,
        current_products=42,
        report_kind="consolidated",
    )
    channel = SimpleNamespace(
        label="primary",
        smtp_server="smtp.example.com",
        smtp_port=465,
        sender_email="sender@example.com",
        smtp_password="not-logged",
        app_proxy_configured=True,
    )

    with patch(
        "scheduler_notifications._configured_smtp_channels",
        return_value=[channel],
    ), patch("scheduler_notifications.smtp_send") as smtp_send:
        assert send_with_smtp_failover(
            object(),
            msg,
            ["first@example.net", "second@example.org"],
            retry_attempts=2,
            purpose="test-report",
        )

    smtp_send.assert_called_once()
    log_text = caplog.text
    assert "SMTP_DIAG message_begin" in log_text
    assert "message_bytes=" in log_text
    assert "recipient_count=2" in log_text
    assert "recipient_domains=example.net,example.org" in log_text
    assert "entry_count=7" in log_text
    assert "new_products=3" in log_text
    assert "removed_products=1" in log_text
    assert "current_products=42" in log_text
    assert "report_kind=consolidated" in log_text
    assert "first@example.net" not in log_text
    assert "second@example.org" not in log_text
    assert "not-logged" not in log_text


def test_public_delivery_error_identifies_message_size_rejection():
    error = smtplib.SMTPDataError(552, b"message too large")

    assert _public_delivery_error(error) == "SMTP 服务器因邮件过大拒绝接收（代码 552）。"


def test_smtp_send_prefers_587_proxy_then_587_direct():
    config = SimpleNamespace(
        label="primary",
        smtp_server="smtp.example.com",
        smtp_port=465,
        sender_email="sender@example.com",
        proxy_url="http://127.0.0.1:7897",
    )
    failure = OSError("proxy connect failed")
    with patch(
        "scheduler_notifications.send_smtp_once",
        side_effect=[failure, None],
    ) as send_once:
        smtp_send(config, "secret", _message(), ["recipient@example.net"], retry_attempts=0)

    assert send_once.call_count == 2
    assert send_once.call_args_list[0].kwargs["port"] == 587
    assert send_once.call_args_list[0].kwargs["proxy_url"] == "http://127.0.0.1:7897"
    assert send_once.call_args_list[1].kwargs["port"] == 587
    assert send_once.call_args_list[1].kwargs["proxy_url"] == ""


def test_smtp_send_uses_short_connect_timeout_and_longer_data_timeout():
    config = SimpleNamespace(
        label="primary",
        smtp_server="smtp.example.com",
        smtp_port=465,
        sender_email="sender@example.com",
        proxy_url="",
    )
    with patch("scheduler_notifications.send_smtp_once") as send_once:
        smtp_send(config, "secret", _message(), ["recipient@example.net"], retry_attempts=0)

    kwargs = send_once.call_args.kwargs
    assert kwargs["timeout"] == scheduler_notifications._SMTP_CONNECT_TIMEOUT_SECONDS
    assert kwargs["timeout"] < 30
    assert kwargs["data_timeout"] == scheduler_notifications._SMTP_DATA_TIMEOUT_SECONDS
    assert kwargs["data_timeout"] > kwargs["timeout"]


def test_is_proxy_fake_ip_matches_only_benchmark_range():
    assert is_proxy_fake_ip("198.18.4.147")
    assert is_proxy_fake_ip("198.19.255.1")
    assert not is_proxy_fake_ip("183.47.120.204")
    assert not is_proxy_fake_ip("142.250.4.108")
    assert not is_proxy_fake_ip("fd00::48b")
    assert not is_proxy_fake_ip("not-an-ip")


def test_fake_ip_dns_marks_failure_as_proxy_intercepted(caplog):
    caplog.set_level(logging.INFO)

    class EofSMTPSSL:
        def __init__(self, *args, **kwargs):
            raise ssl.SSLEOFError(8, "EOF occurred in violation of protocol")

    fake_ip_answer = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("198.18.4.147", 465))]
    with patch("smtp_diagnostics.socket.getaddrinfo", return_value=fake_ip_answer), patch(
        "smtp_diagnostics.DiagnosticSMTPSSL", EofSMTPSSL
    ):
        with pytest.raises(ssl.SSLEOFError) as excinfo:
            send_smtp_once(
                _config(),
                "secret",
                _message(),
                ["recipient@example.net"],
                port=465,
                trace_id="trace-tun",
                channel="primary",
                attempt=1,
                timeout=1,
            )

    assert excinfo.value.proxy_intercepted is True
    assert "proxy_fake_ip=true" in caplog.text
    assert "SMTP_DIAG route_intercepted trace_id=trace-tun" in caplog.text
    assert _public_delivery_error(excinfo.value) == _PROXY_INTERCEPTED_ERROR
    assert _outbox_failure_message(excinfo.value) == _PROXY_INTERCEPTED_ERROR


def test_real_dns_answer_keeps_generic_failure_message(caplog):
    caplog.set_level(logging.INFO)

    class EofSMTPSSL:
        def __init__(self, *args, **kwargs):
            raise ssl.SSLEOFError(8, "EOF occurred in violation of protocol")

    real_answer = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("183.47.120.204", 465))]
    with patch("smtp_diagnostics.socket.getaddrinfo", return_value=real_answer), patch(
        "smtp_diagnostics.DiagnosticSMTPSSL", EofSMTPSSL
    ):
        with pytest.raises(ssl.SSLEOFError) as excinfo:
            send_smtp_once(
                _config(),
                "secret",
                _message(),
                ["recipient@example.net"],
                port=465,
                trace_id="trace-real",
                channel="primary",
                attempt=1,
                timeout=1,
            )

    assert not getattr(excinfo.value, "proxy_intercepted", False)
    assert "proxy_fake_ip=false" in caplog.text
    assert _outbox_failure_message(excinfo.value) == "邮件发送失败，请查看日志后重试。"


def test_failover_tags_final_error_when_earlier_channel_was_intercepted(caplog):
    caplog.set_level(logging.INFO)
    channels = [
        SimpleNamespace(
            label="primary",
            smtp_server="smtp.qq.com",
            smtp_port=465,
            sender_email="primary@qq.com",
            smtp_password="p",
            app_proxy_configured=True,
        ),
        SimpleNamespace(
            label="backup",
            smtp_server="smtp.gmail.com",
            smtp_port=465,
            sender_email="backup@gmail.com",
            smtp_password="b",
            app_proxy_configured=True,
        ),
    ]
    intercepted = TimeoutError("qq timed out")
    intercepted.proxy_intercepted = True
    plain = TimeoutError("gmail timed out")

    with patch("scheduler_notifications._configured_smtp_channels", return_value=channels), patch(
        "scheduler_notifications.smtp_send", side_effect=[intercepted, plain]
    ):
        with pytest.raises(TimeoutError) as excinfo:
            send_with_smtp_failover(object(), _message(), ["ops@example.net"], purpose="test")

    assert excinfo.value is plain
    assert excinfo.value.proxy_intercepted is True
    assert "SMTP_DIAG proxy_intercepted" in caplog.text
    assert "servers=smtp.qq.com" in caplog.text
