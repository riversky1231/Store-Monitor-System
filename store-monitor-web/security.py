import concurrent.futures
import ipaddress
import logging
import os
import re
import secrets
import socket
import sys
import threading
import time
from collections import OrderedDict
from contextlib import closing
from pathlib import Path
from urllib.parse import urlparse

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from utils import get_runtime_base_path

try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:  # pragma: no cover - runtime fallback when optional dep is missing
    Fernet = None
    InvalidToken = Exception

try:
    from email_validator import validate_email as _validate_email_lib, EmailNotValidError
    _EMAIL_VALIDATOR_AVAILABLE = True
except ImportError:  # pragma: no cover - fallback to regex if library not installed
    _EMAIL_VALIDATOR_AVAILABLE = False


logger = logging.getLogger(__name__)

http_basic = HTTPBasic(auto_error=False)

AUTH_DISABLE_ENV = "MONITOR_WEB_DISABLE_AUTH"
AUTH_USERNAME_ENV = "MONITOR_WEB_USERNAME"
AUTH_PASSWORD_ENV = "MONITOR_WEB_PASSWORD"
AUTH_REQUIRE_HTTPS_ENV = "MONITOR_WEB_REQUIRE_HTTPS"
AUTH_RATE_LIMIT_MAX_ATTEMPTS_ENV = "MONITOR_WEB_AUTH_MAX_ATTEMPTS"
AUTH_RATE_LIMIT_WINDOW_SECONDS_ENV = "MONITOR_WEB_AUTH_WINDOW_SECONDS"
AUTH_RATE_LIMIT_MAX_CLIENTS_ENV = "MONITOR_WEB_AUTH_MAX_CLIENTS"
URL_VALIDATION_CACHE_TTL_ENV = "MONITOR_WEB_URL_VALIDATION_CACHE_TTL_SECONDS"
URL_VALIDATION_CACHE_MAX_ENTRIES_ENV = "MONITOR_WEB_URL_VALIDATION_CACHE_MAX_ENTRIES"
DNS_LOOKUP_TIMEOUT_SECONDS_ENV = "MONITOR_WEB_DNS_TIMEOUT_SECONDS"

SMTP_SECRET_KEY_ENV = "STORE_MONITOR_SECRET_KEY"
SMTP_SECRET_FILE_ENV = "STORE_MONITOR_SECRET_FILE"
SMTP_SECRET_FILE_DEFAULT = ".store_monitor_secret.key"
SMTP_SECRET_PREFIX = "enc::"
COMPAT_PROXY_NETWORKS = (
    ipaddress.ip_network("198.18.0.0/15"),
)

# 保留正则作为 email-validator 不可用时的回退
EMAIL_RE = re.compile(r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$")


def is_valid_email(email: str) -> bool:
    """验证邮件地址格式。优先使用 email-validator 库，不可用时回退到正则。"""
    if _EMAIL_VALIDATOR_AVAILABLE:
        try:
            _validate_email_lib(email, check_deliverability=False)
            return True
        except EmailNotValidError:
            return False
    return bool(EMAIL_RE.fullmatch(email))

_fernet_client = None
_fernet_lock = threading.Lock()
_auth_limit_lock = threading.Lock()
_auth_failures: OrderedDict[str, list[float]] = OrderedDict()
_auth_cleanup_counter = 0
_AUTH_CLEANUP_INTERVAL = 100  # Run cleanup every 100 auth attempts
_auth_cache_max_keys = None

_host_validation_lock = threading.Lock()
_host_validation_cache: OrderedDict[str, float] = OrderedDict()

_dns_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="monitor-dns",
)


def require_admin_auth(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(http_basic),
) -> None:
    if os.getenv(AUTH_DISABLE_ENV, "").strip().lower() in {"1", "true", "yes"}:
        return

    if _should_require_https() and not _is_secure_transport(request):
        raise HTTPException(
            status_code=status.HTTP_426_UPGRADE_REQUIRED,
            detail="HTTPS is required for Basic Auth outside localhost.",
        )

    client_key = _build_auth_client_key(request)
    _enforce_auth_rate_limit(client_key)

    expected_username, expected_password = _load_admin_credentials()
    if not expected_password:
        # Setup not complete — redirect via 302 so browser goes to setup page.
        raise HTTPException(
            status_code=status.HTTP_302_FOUND,
            headers={"Location": "/setup"},
        )

    if credentials is None:
        _record_auth_failure(client_key)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": 'Basic realm="Store Monitor"'},
        )

    user_ok = secrets.compare_digest(credentials.username, expected_username)
    pass_ok = secrets.compare_digest(credentials.password, expected_password)
    if not (user_ok and pass_ok):
        _record_auth_failure(client_key)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": 'Basic realm="Store Monitor"'},
        )

    _reset_auth_failures(client_key)


def _load_admin_credentials() -> tuple[str, str]:
    """Return (username, plaintext_password). Password is empty string if not configured."""
    # Env vars take priority (dev/server mode).
    env_password = os.getenv(AUTH_PASSWORD_ENV, "").strip()
    if env_password:
        return os.getenv(AUTH_USERNAME_ENV, "admin"), env_password

    # Fall back to DB-stored encrypted password.
    try:
        from database import SessionLocal
        from models import SystemConfig
        with closing(SessionLocal()) as db:
            config = db.query(SystemConfig).first()
            if config and config.admin_password_enc:
                plaintext = decrypt_secret(config.admin_password_enc)
                return "admin", plaintext
    except Exception as exc:
        logger.warning("Could not load admin credentials from DB: %s", exc)

    return "admin", ""


def _should_require_https() -> bool:
    raw = os.getenv(AUTH_REQUIRE_HTTPS_ENV, "1").strip().lower()
    return raw not in {"0", "false", "no"}


def _is_secure_transport(request: Request) -> bool:
    host = (request.url.hostname or "").lower()
    if host in {"localhost", "127.0.0.1", "::1"}:
        return True

    if (request.url.scheme or "").lower() == "https":
        return True

    forwarded_proto = request.headers.get("x-forwarded-proto", "")
    if forwarded_proto:
        first_proto = forwarded_proto.split(",")[0].strip().lower()
        if first_proto == "https":
            return True
    return False


def _build_auth_client_key(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip() or "unknown"
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _enforce_auth_rate_limit(client_key: str) -> None:
    max_attempts = _read_int_env(AUTH_RATE_LIMIT_MAX_ATTEMPTS_ENV, 5, min_value=1)
    window_seconds = _read_int_env(AUTH_RATE_LIMIT_WINDOW_SECONDS_ENV, 60, min_value=10)
    now = time.time()
    with _auth_limit_lock:
        attempts = _auth_failures.pop(client_key, [])
        attempts = [ts for ts in attempts if now - ts <= window_seconds]
        _auth_failures[client_key] = attempts
        if len(attempts) < max_attempts:
            return

        retry_after = max(1, int(window_seconds - (now - attempts[0])))
        logger.warning(
            "Auth rate limit exceeded for client=%s (attempts=%d/%d).",
            client_key,
            len(attempts),
            max_attempts,
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many authentication failures. Retry in {retry_after} seconds.",
            headers={"Retry-After": str(retry_after)},
        )


def _record_auth_failure(client_key: str) -> None:
    now = time.time()
    window_seconds = _read_int_env(AUTH_RATE_LIMIT_WINDOW_SECONDS_ENV, 60, min_value=10)
    with _auth_limit_lock:
        attempts = _auth_failures.pop(client_key, [])
        attempts = [ts for ts in attempts if now - ts <= window_seconds]
        attempts.append(now)
        _auth_failures[client_key] = attempts
        _prune_auth_failures_locked()
    _maybe_cleanup_auth_failures()


def _reset_auth_failures(client_key: str) -> None:
    with _auth_limit_lock:
        _auth_failures.pop(client_key, None)


def _cleanup_auth_failures() -> None:
    """Remove expired entries from _auth_failures to prevent memory leak."""
    global _auth_cleanup_counter
    window_seconds = _read_int_env(AUTH_RATE_LIMIT_WINDOW_SECONDS_ENV, 60, min_value=10)
    now = time.time()
    expired_keys = [
        key for key, timestamps in _auth_failures.items()
        if not any(now - ts <= window_seconds for ts in timestamps)
    ]
    for key in expired_keys:
        _auth_failures.pop(key, None)
    _prune_auth_failures_locked()


def _maybe_cleanup_auth_failures() -> None:
    """Periodically trigger cleanup to prevent unbounded memory growth."""
    global _auth_cleanup_counter
    _auth_cleanup_counter += 1
    if _auth_cleanup_counter >= _AUTH_CLEANUP_INTERVAL:
        _auth_cleanup_counter = 0
        _cleanup_auth_failures()


def _prune_auth_failures_locked() -> None:
    """Cap auth failure cache size to prevent unbounded growth."""
    max_keys = _get_auth_cache_max_keys()
    while len(_auth_failures) > max_keys:
        _auth_failures.popitem(last=False)


def _get_auth_cache_max_keys() -> int:
    global _auth_cache_max_keys
    if _auth_cache_max_keys is None:
        _auth_cache_max_keys = _read_int_env(AUTH_RATE_LIMIT_MAX_CLIENTS_ENV, 2000, min_value=100)
    return _auth_cache_max_keys


def _read_int_env(name: str, default: int, min_value: int = 1) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        logger.warning("Invalid int env %s=%s, fallback=%d", name, raw, default)
        return default
    if parsed < min_value:
        return default
    return parsed


def normalize_recipients(raw_value: str) -> str:
    recipients = []
    for candidate in raw_value.split(","):
        email = candidate.strip()
        if not email:
            continue
        if "\r" in email or "\n" in email:
            raise ValueError("Recipient email contains invalid control characters.")
        if not is_valid_email(email):
            raise ValueError(f"Invalid recipient email format: {email}")
        recipients.append(email)

    if not recipients:
        raise ValueError("At least one valid recipient email is required.")

    # Keep order while removing duplicates.
    unique = list(dict.fromkeys(recipients))
    return ", ".join(unique)


def validate_monitor_target_url(raw_url: str) -> str:
    candidate = (raw_url or "").strip()
    parsed = urlparse(candidate)

    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http/https URLs are allowed.")
    if not parsed.hostname:
        raise ValueError("URL must contain a valid host.")
    if parsed.username or parsed.password:
        raise ValueError("Embedded credentials are not allowed in target URLs.")

    hostname = parsed.hostname.strip().lower().rstrip(".")
    if hostname in {"localhost"} or hostname.endswith(".local"):
        raise ValueError("Local/private hosts are not allowed.")
    if "." not in hostname:
        raise ValueError("Target host must be a public domain name.")

    _ensure_public_host(hostname)
    return parsed.geturl()


def encrypt_secret(value: str) -> str:
    plaintext = (value or "").strip()
    if not plaintext:
        return ""

    fernet_client = _get_fernet_client()
    if not fernet_client:
        raise RuntimeError(
            "Secure SMTP password storage is unavailable. "
            "Install 'cryptography' or set SMTP password via environment variable."
        )

    token = fernet_client.encrypt(plaintext.encode("utf-8")).decode("utf-8")
    return f"{SMTP_SECRET_PREFIX}{token}"


def decrypt_secret(value: str) -> str:
    stored = (value or "").strip()
    if not stored:
        return ""

    if not stored.startswith(SMTP_SECRET_PREFIX):
        return stored

    fernet_client = _get_fernet_client()
    if not fernet_client:
        logger.error("Encrypted SMTP password exists but encryption backend is unavailable.")
        return ""

    token = stored[len(SMTP_SECRET_PREFIX) :]
    try:
        return fernet_client.decrypt(token.encode("utf-8")).decode("utf-8")
    except InvalidToken:
        logger.error("Failed to decrypt SMTP password. Check secret key configuration.")
        return ""


def _ensure_public_host(hostname: str) -> None:
    if _is_host_cached(hostname):
        return
    literal_ip = _safe_parse_ip(hostname)
    if literal_ip:
        if _is_blocked_ip(literal_ip):
            raise ValueError("Private/loopback IP targets are not allowed.")
        return

    infos = _resolve_host_infos(hostname)

    resolved = {item[4][0] for item in infos if item and item[4]}
    if not resolved:
        raise ValueError(f"Unable to resolve host: {hostname}")

    for addr in resolved:
        ip_obj = _safe_parse_ip(addr)
        if not ip_obj:
            continue
        if _is_blocked_ip(ip_obj):
            raise ValueError("Resolved host points to a private/loopback network.")
    _cache_public_host(hostname)


def _resolve_host_infos(hostname: str):
    timeout_seconds = _read_int_env(DNS_LOOKUP_TIMEOUT_SECONDS_ENV, 5, min_value=1)
    future = _dns_executor.submit(socket.getaddrinfo, hostname, None, proto=socket.IPPROTO_TCP)
    try:
        return future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError as exc:
        future.cancel()
        raise ValueError(f"DNS lookup timed out for host: {hostname}") from exc
    except socket.gaierror as exc:
        raise ValueError(f"Unable to resolve host: {hostname}") from exc
    except Exception as exc:
        raise ValueError(f"DNS lookup failed for host: {hostname}") from exc


def _is_host_cached(hostname: str) -> bool:
    ttl_seconds = _read_int_env(URL_VALIDATION_CACHE_TTL_ENV, 300, min_value=0)
    if ttl_seconds <= 0:
        return False
    now = time.time()
    with _host_validation_lock:
        cached_at = _host_validation_cache.get(hostname)
        if cached_at is None:
            return False
        if now - cached_at > ttl_seconds:
            _host_validation_cache.pop(hostname, None)
            return False
        _host_validation_cache.move_to_end(hostname)
        return True


def _cache_public_host(hostname: str) -> None:
    ttl_seconds = _read_int_env(URL_VALIDATION_CACHE_TTL_ENV, 300, min_value=0)
    if ttl_seconds <= 0:
        return
    max_entries = _read_int_env(URL_VALIDATION_CACHE_MAX_ENTRIES_ENV, 1024, min_value=10)
    with _host_validation_lock:
        _host_validation_cache[hostname] = time.time()
        _host_validation_cache.move_to_end(hostname)
        while len(_host_validation_cache) > max_entries:
            _host_validation_cache.popitem(last=False)


def _safe_parse_ip(value: str):
    try:
        return ipaddress.ip_address(value)
    except ValueError:
        return None


def _is_blocked_ip(ip_obj: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    # Some enterprise/proxy networks resolve public domains to RFC 2544 test net
    # addresses (198.18.0.0/15) and route them through a local gateway.
    if any(ip_obj in network for network in COMPAT_PROXY_NETWORKS):
        return False
    return (
        (not ip_obj.is_global)
        or ip_obj.is_private
        or ip_obj.is_loopback
        or ip_obj.is_link_local
        or ip_obj.is_multicast
        or ip_obj.is_reserved
        or ip_obj.is_unspecified
    )


def _get_fernet_client():
    global _fernet_client
    if _fernet_client is not None:
        return _fernet_client
    if Fernet is None:
        return None

    with _fernet_lock:
        if _fernet_client is not None:
            return _fernet_client
        key_bytes = _load_or_create_secret_key()
        try:
            _fernet_client = Fernet(key_bytes)
        except Exception as exc:  # pragma: no cover - invalid key is runtime configuration issue
            raise RuntimeError(
                f"Invalid encryption key. Set '{SMTP_SECRET_KEY_ENV}' to a valid Fernet key."
            ) from exc
    return _fernet_client


def _load_or_create_secret_key() -> bytes:
    env_key = os.getenv(SMTP_SECRET_KEY_ENV, "").strip()
    if env_key:
        return env_key.encode("utf-8")

    configured_key_path = os.getenv(SMTP_SECRET_FILE_ENV, "").strip()
    if configured_key_path:
        return _read_or_create_key_at(Path(configured_key_path))

    runtime_default_key_path = get_runtime_base_path() / SMTP_SECRET_FILE_DEFAULT
    if runtime_default_key_path.exists():
        return runtime_default_key_path.read_bytes().strip()

    cwd_default_key_path = Path.cwd() / SMTP_SECRET_FILE_DEFAULT
    if cwd_default_key_path.exists():
        key_bytes = cwd_default_key_path.read_bytes().strip()
        if key_bytes and cwd_default_key_path != runtime_default_key_path:
            _write_key_file(runtime_default_key_path, key_bytes)
        return key_bytes

    # Compatibility: migrate legacy key locations to the stable runtime path.
    legacy_paths = []
    if getattr(sys, "frozen", False):
        legacy_paths.append(Path(sys.executable).resolve().parent / SMTP_SECRET_FILE_DEFAULT)
    legacy_paths.append(Path(__file__).resolve().parent / SMTP_SECRET_FILE_DEFAULT)
    legacy_paths.append(cwd_default_key_path)

    for legacy_path in legacy_paths:
        if legacy_path == runtime_default_key_path or not legacy_path.exists():
            continue
        key_bytes = legacy_path.read_bytes().strip()
        if key_bytes:
            _write_key_file(runtime_default_key_path, key_bytes)
            return key_bytes

    generated_key = Fernet.generate_key()
    _write_key_file(runtime_default_key_path, generated_key)
    return generated_key


def _read_or_create_key_at(key_path: Path) -> bytes:
    if key_path.exists():
        return key_path.read_bytes().strip()

    generated_key = Fernet.generate_key()
    _write_key_file(key_path, generated_key)
    return generated_key


def _write_key_file(key_path: Path, key_bytes: bytes) -> None:
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_bytes(key_bytes)
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        # Permission tightening is best-effort on non-POSIX systems.
        pass
