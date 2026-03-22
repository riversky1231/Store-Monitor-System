import os
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.security import HTTPBasicCredentials
from starlette.requests import Request

import security
from security import (
    AUTH_PASSWORD_ENV,
    AUTH_RATE_LIMIT_MAX_ATTEMPTS_ENV,
    AUTH_RATE_LIMIT_WINDOW_SECONDS_ENV,
    AUTH_REQUIRE_HTTPS_ENV,
    SMTP_SECRET_FILE_DEFAULT,
    SMTP_SECRET_FILE_ENV,
    SMTP_SECRET_KEY_ENV,
    AUTH_USERNAME_ENV,
    is_valid_email,
    normalize_recipients,
    require_admin_auth,
    validate_monitor_target_url,
)

_PUBLIC_DNS = [(None, None, None, None, ("93.184.216.34", 0))]
_PRIVATE_DNS = [(None, None, None, None, ("192.168.1.1", 0))]
_LOOPBACK_DNS = [(None, None, None, None, ("127.0.0.1", 0))]
_COMPAT_PROXY_DNS = [(None, None, None, None, ("198.18.0.4", 0))]


def _build_request(host: str, scheme: str = "http", client_ip: str = "198.51.100.7") -> Request:
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": scheme,
        "path": "/",
        "raw_path": b"/",
        "root_path": "",
        "query_string": b"",
        "headers": [],
        "client": (client_ip, 12345),
        "server": (host, 80 if scheme == "http" else 443),
    }
    return Request(scope)


class SecurityTests(unittest.TestCase):
    def setUp(self):
        self._env_backup = dict(os.environ)
        security._auth_failures.clear()
        security._host_validation_cache.clear()
        security._auth_cache_max_keys = None
        os.environ[AUTH_USERNAME_ENV] = "admin"
        os.environ[AUTH_PASSWORD_ENV] = "secret-pass"
        os.environ[AUTH_RATE_LIMIT_MAX_ATTEMPTS_ENV] = "5"
        os.environ[AUTH_RATE_LIMIT_WINDOW_SECONDS_ENV] = "60"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env_backup)
        security._auth_failures.clear()
        security._host_validation_cache.clear()
        security._auth_cache_max_keys = None
        security._fernet_client = None

    def test_normalize_recipients_deduplicates(self):
        actual = normalize_recipients("a@test.com, b@test.com, a@test.com")
        self.assertEqual(actual, "a@test.com, b@test.com")

    def test_normalize_recipients_rejects_invalid(self):
        with self.assertRaises(ValueError):
            normalize_recipients("valid@test.com, bad-email")

    def test_require_admin_auth_enforces_https_for_non_localhost(self):
        os.environ[AUTH_REQUIRE_HTTPS_ENV] = "1"
        request = _build_request(host="monitor.example.com", scheme="http")
        credentials = HTTPBasicCredentials(username="admin", password="secret-pass")

        with self.assertRaises(HTTPException) as ctx:
            require_admin_auth(request=request, credentials=credentials)
        self.assertEqual(ctx.exception.status_code, 426)

    def test_require_admin_auth_rate_limits_after_failed_attempts(self):
        os.environ[AUTH_REQUIRE_HTTPS_ENV] = "0"
        request = _build_request(host="monitor.example.com", scheme="http")
        wrong_credentials = HTTPBasicCredentials(username="admin", password="wrong")

        for _ in range(5):
            with self.assertRaises(HTTPException) as ctx:
                require_admin_auth(request=request, credentials=wrong_credentials)
            self.assertEqual(ctx.exception.status_code, 401)

        with self.assertRaises(HTTPException) as ctx:
            require_admin_auth(request=request, credentials=wrong_credentials)
        self.assertEqual(ctx.exception.status_code, 429)

    def test_secret_key_file_defaults_to_runtime_directory(self):
        os.environ.pop(SMTP_SECRET_KEY_ENV, None)
        os.environ.pop(SMTP_SECRET_FILE_ENV, None)
        security._fernet_client = None

        old_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                with patch("security.get_runtime_base_path", return_value=Path(tmpdir)):
                    key1 = security._load_or_create_secret_key()
                    expected_path = Path(tmpdir) / SMTP_SECRET_FILE_DEFAULT
                    self.assertTrue(expected_path.exists())
                    self.assertEqual(expected_path.read_bytes().strip(), key1)

                    key2 = security._load_or_create_secret_key()
                    self.assertEqual(key1, key2)
            finally:
                os.chdir(old_cwd)


class ValidateMonitorTargetUrlTests(unittest.TestCase):
    def setUp(self):
        security._host_validation_cache.clear()

    def tearDown(self):
        security._host_validation_cache.clear()

    def _valid(self, url: str) -> str:
        with patch("security.socket.getaddrinfo", return_value=_PUBLIC_DNS):
            return validate_monitor_target_url(url)

    def test_valid_https_accepted(self):
        result = self._valid("https://www.amazon.com/s?me=A123")
        self.assertTrue(result.startswith("https://www.amazon.com"))

    def test_valid_http_accepted(self):
        result = self._valid("http://shop.example.com/new-arrivals")
        self.assertTrue(result.startswith("http://shop.example.com"))

    def test_ftp_scheme_rejected(self):
        with self.assertRaisesRegex(ValueError, "http/https"):
            validate_monitor_target_url("ftp://example.com/file")

    def test_javascript_scheme_rejected(self):
        with self.assertRaisesRegex(ValueError, "http/https"):
            validate_monitor_target_url("javascript:alert(1)")

    def test_missing_host_rejected(self):
        with self.assertRaisesRegex(ValueError, "valid host"):
            validate_monitor_target_url("https://")

    def test_localhost_rejected(self):
        with self.assertRaisesRegex(ValueError, "private"):
            validate_monitor_target_url("http://localhost/admin")

    def test_dotlocal_rejected(self):
        with self.assertRaisesRegex(ValueError, "private"):
            validate_monitor_target_url("http://printer.local/status")

    def test_bare_hostname_rejected(self):
        with self.assertRaisesRegex(ValueError, "public domain"):
            validate_monitor_target_url("http://internalserver/page")

    def test_embedded_credentials_rejected(self):
        with self.assertRaisesRegex(ValueError, "credentials"):
            validate_monitor_target_url("https://user:pass@example.com/page")

    def test_private_ip_via_dns_rejected(self):
        with patch("security.socket.getaddrinfo", return_value=_PRIVATE_DNS):
            with self.assertRaisesRegex(ValueError, "private"):
                validate_monitor_target_url("https://evil.example.com/")

    def test_loopback_ip_via_dns_rejected(self):
        with patch("security.socket.getaddrinfo", return_value=_LOOPBACK_DNS):
            with self.assertRaisesRegex(ValueError, "private"):
                validate_monitor_target_url("https://loopback.example.com/")

    def test_compat_proxy_ip_via_dns_allowed(self):
        with patch("security.socket.getaddrinfo", return_value=_COMPAT_PROXY_DNS):
            result = validate_monitor_target_url("https://proxy-mapped.example.com/")
        self.assertEqual(result, "https://proxy-mapped.example.com/")

    def test_literal_private_ip_rejected(self):
        with self.assertRaisesRegex(ValueError, "(?i)private"):
            validate_monitor_target_url("http://192.168.0.1/admin")

    def test_literal_loopback_ip_rejected(self):
        with self.assertRaisesRegex(ValueError, "(?i)private"):
            validate_monitor_target_url("http://127.0.0.1:8080/")

    def test_dns_failure_rejected(self):
        with patch("security.socket.getaddrinfo", side_effect=socket.gaierror("NXDOMAIN")):
            with self.assertRaisesRegex(ValueError, "resolve host"):
                validate_monitor_target_url("https://nonexistent.invalid/")

    def test_empty_url_rejected(self):
        with self.assertRaises(ValueError):
            validate_monitor_target_url("")


class IsValidEmailTests(unittest.TestCase):
    """测试 is_valid_email() 函数的邮件地址格式验证。"""

    def test_standard_email_accepted(self):
        self.assertTrue(is_valid_email("user@example.com"))

    def test_subdomain_email_accepted(self):
        self.assertTrue(is_valid_email("user@mail.example.co.uk"))

    def test_plus_tag_email_accepted(self):
        self.assertTrue(is_valid_email("user+tag@example.com"))

    def test_missing_at_sign_rejected(self):
        self.assertFalse(is_valid_email("userexample.com"))

    def test_missing_domain_rejected(self):
        self.assertFalse(is_valid_email("user@"))

    def test_missing_local_part_rejected(self):
        self.assertFalse(is_valid_email("@example.com"))

    def test_empty_string_rejected(self):
        self.assertFalse(is_valid_email(""))

    def test_no_tld_rejected(self):
        self.assertFalse(is_valid_email("user@localhost"))

    def test_control_char_rejected(self):
        self.assertFalse(is_valid_email("user\r@example.com"))

    def test_is_valid_email_falls_back_to_regex_when_library_unavailable(self):
        """当 email-validator 不可用时，应回退到正则验证。"""
        with patch.object(security, "_EMAIL_VALIDATOR_AVAILABLE", False):
            self.assertTrue(is_valid_email("fallback@example.com"))
            self.assertFalse(is_valid_email("bad-email"))


if __name__ == "__main__":
    unittest.main()
