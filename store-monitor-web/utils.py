import datetime
import re
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


_BLOCK_RESPONSE_KEYWORDS = (
    "captcha",
    "robot check",
    "unusual traffic",
    "automated access",
    "type the characters",
    "validatecaptcha",
)


def get_runtime_base_path() -> Path:
    """Return the stable writable application directory."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _normalized_relative_path(relative_path: str) -> Path:
    normalized = (relative_path or "").replace("\\", "/").strip()
    candidate = Path(normalized)
    if not normalized:
        raise ValueError("relative_path cannot be empty.")
    if candidate.is_absolute() or re.match(r"^[A-Za-z]:/", normalized):
        raise ValueError(f"Invalid relative_path: path traversal not allowed: {relative_path}")
    if any(part == ".." for part in candidate.parts):
        raise ValueError(f"Invalid relative_path: path traversal not allowed: {relative_path}")
    return candidate


@dataclass(frozen=True)
class HttpProbeResult:
    status_code: int | None
    body_text: str
    elapsed_ms: int | None
    final_url: str
    error_kind: str | None = None
    error_message: str = ""


def get_resource_path(relative_path: str) -> str:
    """Get absolute path to resource, works for dev and for PyInstaller.

    Args:
        relative_path: The relative path to the resource. Must not contain
                      path traversal sequences (../ or absolute paths).

    Returns:
        Absolute path to the resource within the base directory.

    Raises:
        ValueError: If relative_path contains path traversal sequences.
    """
    rel_path = _normalized_relative_path(relative_path)

    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = Path(sys._MEIPASS)
    except Exception:
        base_path = get_runtime_base_path()

    full_path = (base_path / rel_path).resolve()
    base_real = base_path.resolve()
    try:
        full_path.relative_to(base_real)
    except ValueError as exc:
        raise ValueError(f"Invalid relative_path: path traversal not allowed: {relative_path}") from exc

    return str(full_path)


def probe_http_text(
    url: str,
    timeout: int = 15,
    headers: dict[str, str] | None = None,
    max_bytes: int = 4096,
) -> HttpProbeResult:
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    if headers:
        request_headers.update(headers)

    request = Request(url, headers=request_headers, method="GET")
    start = time.time()
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read(max_bytes)
            elapsed_ms = round((time.time() - start) * 1000)
            charset = response.headers.get_content_charset() or "utf-8"
            return HttpProbeResult(
                status_code=response.getcode(),
                body_text=payload.decode(charset, errors="ignore"),
                elapsed_ms=elapsed_ms,
                final_url=response.geturl(),
            )
    except HTTPError as exc:
        payload = exc.read(max_bytes)
        elapsed_ms = round((time.time() - start) * 1000)
        charset = exc.headers.get_content_charset() if exc.headers else None
        return HttpProbeResult(
            status_code=exc.code,
            body_text=payload.decode(charset or "utf-8", errors="ignore"),
            elapsed_ms=elapsed_ms,
            final_url=getattr(exc, "url", url),
            error_kind="http_error",
            error_message=str(exc),
        )
    except URLError as exc:
        reason = getattr(exc, "reason", exc)
        is_timeout = isinstance(reason, socket.timeout)
        return HttpProbeResult(
            status_code=None,
            body_text="",
            elapsed_ms=None,
            final_url=url,
            error_kind="timeout" if is_timeout else "url_error",
            error_message=str(reason),
        )
    except socket.timeout as exc:
        return HttpProbeResult(
            status_code=None,
            body_text="",
            elapsed_ms=None,
            final_url=url,
            error_kind="timeout",
            error_message=str(exc),
        )
    except Exception as exc:
        return HttpProbeResult(
            status_code=None,
            body_text="",
            elapsed_ms=None,
            final_url=url,
            error_kind="error",
            error_message=str(exc),
        )


def response_looks_blocked(url: str, body_text: str) -> bool:
    lowered = (body_text or "").lower()
    if "robots.txt" in (url or "").lower():
        return "captcha" in lowered
    return any(keyword in lowered for keyword in _BLOCK_RESPONSE_KEYWORDS)


def to_beijing_time(dt: datetime.datetime | None) -> str:
    """Convert UTC datetime to Beijing time (UTC+8), return '' if None."""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    beijing_tz = datetime.timezone(datetime.timedelta(hours=8))
    return dt.astimezone(beijing_tz).strftime('%m-%d %H:%M')
