from __future__ import annotations

import datetime
import hashlib
import html
import json
import logging
import os
import random
import re
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Set, Tuple
from urllib.parse import (
    urljoin,
    urlparse,
    parse_qs,
    parse_qsl,
    urlencode,
    urlunparse,
    urlsplit,
    urlunsplit,
    unquote,
)

from sqlalchemy.orm import Session

from models import MonitorTask, ProductItem
from utils import get_runtime_base_path

if TYPE_CHECKING:  # only for type checkers; not imported at runtime
    from playwright.sync_api import Locator, Page

logger = logging.getLogger(__name__)

AMAZON_ASIN_RE = re.compile(r"/(?:dp|gp/product)/([A-Z0-9]{10})", re.IGNORECASE)
_MAX_CONTEXT_ATTEMPTS = int(os.getenv("MONITOR_WEB_NAV_ATTEMPTS", "3") or "3")
_MAX_PAGES_DEFAULT = int(os.getenv("MONITOR_WEB_MAX_PAGES", "20") or "20")
_cancel_event = threading.Event()
_MIN_BASELINE_PRODUCTS = max(1, int(os.getenv("MONITOR_WEB_MIN_BASELINE_PRODUCTS", "1") or "1"))
_INTEGRITY_MIN_REFERENCE_COUNT = max(1, int(os.getenv("MONITOR_WEB_INTEGRITY_MIN_REFERENCE_COUNT", "3") or "3"))
_REMOVAL_MISS_THRESHOLD = max(1, int(os.getenv("MONITOR_WEB_REMOVAL_MISS_THRESHOLD", "2") or "2"))
_INTEGRITY_RATIO_SMALL = min(0.98, max(0.55, float(os.getenv("MONITOR_WEB_INTEGRITY_RATIO_SMALL", "0.75") or "0.75")))
_INTEGRITY_RATIO_MEDIUM = min(0.98, max(0.55, float(os.getenv("MONITOR_WEB_INTEGRITY_RATIO_MEDIUM", "0.85") or "0.85")))
_INTEGRITY_RATIO_LARGE = min(0.99, max(0.6, float(os.getenv("MONITOR_WEB_INTEGRITY_RATIO_LARGE", "0.90") or "0.90")))
_RESULT_WAIT_TIMEOUT_MS = max(5000, int(os.getenv("MONITOR_WEB_RESULT_WAIT_MS", "25000") or "25000"))
_EMPTY_PAGE_RECOVERY_ATTEMPTS = max(
    1, int(os.getenv("MONITOR_WEB_EMPTY_PAGE_RECOVERY_ATTEMPTS", "2") or "2")
)
_SESSION_STATE_TTL_HOURS = max(1, int(os.getenv("MONITOR_WEB_SESSION_TTL_HOURS", "24") or "24"))
_DEFAULT_US_ZIP = "10001"
_TRACK_REMOVALS = os.getenv("MONITOR_WEB_TRACK_REMOVALS", "").strip().lower() in ("1", "true", "yes")
_CATALOG_SHIFT_MIN_REFERENCE_COUNT = max(
    5, int(os.getenv("MONITOR_WEB_CATALOG_SHIFT_MIN_REFERENCE_COUNT", "12") or "12")
)
_CATALOG_SHIFT_OVERLAP_RATIO = min(
    0.9,
    max(0.05, float(os.getenv("MONITOR_WEB_CATALOG_SHIFT_OVERLAP_RATIO", "0.45") or "0.45")),
)
_CATALOG_SHIFT_CHANGE_RATIO = min(
    0.95,
    max(0.1, float(os.getenv("MONITOR_WEB_CATALOG_SHIFT_CHANGE_RATIO", "0.35") or "0.35")),
)
_CATALOG_SHIFT_CONFIRM_HOURS = max(
    1, int(os.getenv("MONITOR_WEB_CATALOG_SHIFT_CONFIRM_HOURS", "48") or "48")
)
_CATALOG_SHIFT_CONFIRM_SIMILARITY = min(
    0.99,
    max(
        0.5,
        float(os.getenv("MONITOR_WEB_CATALOG_SHIFT_CONFIRM_SIMILARITY", "0.85") or "0.85"),
    ),
)
_QUERY_PARAM_PAGINATION_ENABLED = os.getenv("MONITOR_WEB_FORCE_QUERY_PAGINATION", "").strip().lower() in (
    "1",
    "true",
    "yes",
)

_BLOCK_KEYWORDS = (
    "captcha",
    "robot check",
    "type the characters",
    "make sure you're not a robot",
    "enter the characters you see below",
    "automated access",
    "unusual traffic",
    "validatecaptcha",
    "something went wrong",
    "service unavailable",
    "503",
)

# Product titles that match these patterns are Amazon promotions, not real products.
_NOISE_TITLE_PATTERNS = (
    "amazon secured card",
    "amazon business card",
    "amazon prime",
    "amazon store card",
    "gift card",
    "gift cards",
    "balance reload",
    "reload your balance",
    "reload your gift",
    "auto-reload",
    "sponsored",
    "amazon product b",  # Generic placeholder names like "Amazon Product B09XXX"
)

_RESULT_SIGNAL_SELECTORS = (
    "div.s-result-item[data-asin]:not([data-asin=''])",
    "div[data-component-type='s-search-result']",
    "div[data-asin]:not([data-asin=''])",
    "a[href*='/dp/']",
    "a[href*='/gp/product/']",
)

_OVERLAY_DISMISS_SELECTORS = (
    "#sp-cc-accept",
    "input#sp-cc-accept",
    "input[name='accept']",
    "button:has-text('Accept Cookies')",
    "button:has-text('Accept')",
)


class ScrapeCancelled(Exception):
    """Raised when a user explicitly cancels a running scrape."""
    pass


class ScrapeTransientError(Exception):
    """Raised for transient browser/network issues that should be retried."""
    pass


def _runtime_base_dir() -> Path:
    return get_runtime_base_path()


def _state_dir() -> Path:
    state_dir = _runtime_base_dir() / "playwright-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir


def _storage_state_path(task_url: str) -> Path:
    parsed = urlparse(task_url)
    host = re.sub(r"[^a-z0-9.-]+", "-", (parsed.hostname or "target").lower()).strip("-") or "target"
    fingerprint = hashlib.sha1(task_url.encode("utf-8")).hexdigest()[:10]
    return _state_dir() / f"{host}-{fingerprint}.json"


def _fresh_storage_state(task_url: str) -> str | None:
    path = _storage_state_path(task_url)
    if not path.exists():
        return None
    max_age = _SESSION_STATE_TTL_HOURS * 3600
    age_seconds = time.time() - path.stat().st_mtime
    if age_seconds > max_age:
        try:
            path.unlink()
        except OSError as exc:
            logger.debug("Failed to remove stale storage state %s: %s", path, exc)
        return None
    return str(path)


def _save_storage_state(context, task_url: str) -> None:
    path = _storage_state_path(task_url)
    try:
        context.storage_state(path=str(path))
    except Exception as exc:
        logger.debug("Failed to persist Playwright storage state %s: %s", path, exc)


def _clear_storage_state(task_url: str) -> None:
    path = _storage_state_path(task_url)
    if not path.exists():
        return
    try:
        path.unlink()
    except OSError as exc:
        logger.debug("Failed to delete storage state %s: %s", path, exc)


def _catalog_shift_state_dir() -> Path:
    state_dir = _runtime_base_dir() / "catalog-shift-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir


def _catalog_shift_state_path(task_id: int) -> Path:
    return _catalog_shift_state_dir() / f"task-{task_id}.json"


def _clear_catalog_shift_state(task_id: int) -> None:
    path = _catalog_shift_state_path(task_id)
    if not path.exists():
        return
    try:
        path.unlink()
    except OSError as exc:
        logger.debug("Failed to delete catalog shift state %s: %s", path, exc)


def _load_catalog_shift_state(task_id: int) -> Dict[str, object] | None:
    path = _catalog_shift_state_path(task_id)
    if not path.exists():
        return None
    max_age = _CATALOG_SHIFT_CONFIRM_HOURS * 3600
    age_seconds = time.time() - path.stat().st_mtime
    if age_seconds > max_age:
        _clear_catalog_shift_state(task_id)
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as exc:
        logger.debug("Failed to load catalog shift state %s: %s", path, exc)
        _clear_catalog_shift_state(task_id)
        return None
    if not isinstance(payload, dict):
        _clear_catalog_shift_state(task_id)
        return None
    return payload


def _save_catalog_shift_state(task_id: int, identity_keys: Set[str]) -> None:
    path = _catalog_shift_state_path(task_id)
    payload = {
        "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "keys": sorted(identity_keys),
    }
    try:
        with path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=True, sort_keys=True)
    except Exception as exc:
        logger.debug("Failed to persist catalog shift state %s: %s", path, exc)


def _product_identity_key(asin: str | None, link: str | None) -> str:
    asin_value = (asin or "").strip().upper()
    if asin_value:
        return asin_value
    return (link or "").strip()


def _catalog_shift_similarity(current_keys: Set[str], pending_keys: Set[str]) -> float:
    if not current_keys or not pending_keys:
        return 0.0
    overlap = len(current_keys & pending_keys)
    return overlap / max(len(current_keys), len(pending_keys))


def _pending_catalog_shift_confirmation(
    task_id: int,
    reference_count: int,
    active_db_products: List[ProductItem],
    current_products: List[Dict[str, str]],
) -> str | None:
    if reference_count < _CATALOG_SHIFT_MIN_REFERENCE_COUNT:
        _clear_catalog_shift_state(task_id)
        return None
    if not active_db_products or not current_products:
        return None

    active_keys = {
        _product_identity_key(p.asin, p.product_link)
        for p in active_db_products
        if _product_identity_key(p.asin, p.product_link)
    }
    current_keys = {
        _product_identity_key(item.get("asin"), item.get("link"))
        for item in current_products
        if _product_identity_key(item.get("asin"), item.get("link"))
    }
    if not active_keys or not current_keys:
        return None

    overlap_count = len(active_keys & current_keys)
    overlap_ratio = overlap_count / max(1, min(len(active_keys), len(current_keys)))
    new_ratio = len(current_keys - active_keys) / max(1, reference_count)
    missing_ratio = len(active_keys - current_keys) / max(1, reference_count)
    change_ratio = max(new_ratio, missing_ratio)

    if overlap_ratio >= _CATALOG_SHIFT_OVERLAP_RATIO or change_ratio < _CATALOG_SHIFT_CHANGE_RATIO:
        _clear_catalog_shift_state(task_id)
        return None

    pending_state = _load_catalog_shift_state(task_id)
    if pending_state:
        pending_keys_raw = pending_state.get("keys")
        pending_keys = set(pending_keys_raw) if isinstance(pending_keys_raw, list) else set()
        similarity = _catalog_shift_similarity(current_keys, pending_keys)
        if similarity >= _CATALOG_SHIFT_CONFIRM_SIMILARITY:
            logger.warning(
                "[ATOMIC] Task %s: accepting high-change catalog shift after confirmation "
                "(overlap=%.2f, change=%.2f, similarity=%.2f).",
                task_id,
                overlap_ratio,
                change_ratio,
                similarity,
            )
            _clear_catalog_shift_state(task_id)
            return None

    _save_catalog_shift_state(task_id, current_keys)
    return (
        "catalog shift pending confirmation "
        f"(overlap={overlap_ratio:.2f}, change={change_ratio:.2f}, "
        f"current={len(current_keys)}, previous={len(active_keys)})"
    )


class _ScrapeRuntime:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._partial_results: List[Dict[str, str]] = []
        self._partial_keys: Set[str] = set()
        self._last_activity_time = time.time()

    def touch(self) -> None:
        with self._lock:
            self._last_activity_time = time.time()

    def add_partial(self, product: Dict[str, str]) -> None:
        key = (product.get("asin") or product.get("link") or product.get("name") or "").strip()
        if not key:
            return
        with self._lock:
            if key in self._partial_keys:
                return
            self._partial_keys.add(key)
            self._partial_results.append(dict(product))
            self._last_activity_time = time.time()

    def partial_results(self) -> List[Dict[str, str]]:
        with self._lock:
            return list(self._partial_results)

    def seconds_since_activity(self) -> float:
        with self._lock:
            return time.time() - self._last_activity_time


_scrape_runtime_local = threading.local()


def _set_scrape_runtime(runtime: _ScrapeRuntime) -> None:
    _scrape_runtime_local.current = runtime


def _clear_scrape_runtime() -> None:
    if hasattr(_scrape_runtime_local, "current"):
        delattr(_scrape_runtime_local, "current")


def _current_scrape_runtime() -> _ScrapeRuntime | None:
    return getattr(_scrape_runtime_local, "current", None)


def _is_noise_title(title: str | None) -> bool:
    if not title:
        return False
    lowered = title.strip().lower()
    return any(noise in lowered for noise in _NOISE_TITLE_PATTERNS)


def request_cancel_scrape() -> None:
    """Request the current scrape to stop as soon as possible."""
    _cancel_event.set()


def clear_cancel_scrape() -> None:
    """Clear any pending cancel request (used as a safety fallback)."""
    _cancel_event.clear()


def is_cancel_requested() -> bool:
    """Return True when a scrape cancel request is pending."""
    return _cancel_event.is_set()


def _check_cancel() -> None:
    if _cancel_event.is_set():
        _cancel_event.clear()
        raise ScrapeCancelled("Scrape cancelled by user.")


def _launch_browser(playwright):
    headful = os.getenv("MONITOR_WEB_HEADFUL", "").strip().lower() in ("1", "true", "yes")
    launch_kwargs = {
        "headless": not headful,
        "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"],
    }
    launch_env = _playwright_launch_env()
    if launch_env:
        launch_kwargs["env"] = launch_env
    try:
        return playwright.chromium.launch(
            channel="chrome",
            **launch_kwargs,
        )
    except Exception as exc:
        logger.warning("Launch system Chrome failed, fallback to bundled Chromium: %s", exc)
        return playwright.chromium.launch(**launch_kwargs)


def _playwright_library_dirs() -> List[str]:
    candidates: List[Path] = []
    override = (os.getenv("MONITOR_WEB_PLAYWRIGHT_LD_LIBRARY_PATH") or "").strip()
    if override:
        for item in override.split(os.pathsep):
            if item.strip():
                candidates.append(Path(item.strip()).expanduser())

    runtime_base = _runtime_base_dir()
    candidates.extend(
        [
            runtime_base / "playwright-libs" / "usr" / "lib" / "x86_64-linux-gnu",
            runtime_base / "playwright-libs" / "lib" / "x86_64-linux-gnu",
            Path.home() / ".local" / "playwright-libs" / "usr" / "lib" / "x86_64-linux-gnu",
            Path.home() / ".local" / "playwright-libs" / "lib" / "x86_64-linux-gnu",
        ]
    )

    ordered: List[str] = []
    for candidate in candidates:
        try:
            if candidate.exists():
                path_text = str(candidate.resolve())
                if path_text not in ordered:
                    ordered.append(path_text)
        except OSError:
            continue
    return ordered


def _playwright_launch_env() -> Dict[str, str] | None:
    library_dirs = _playwright_library_dirs()
    if not library_dirs:
        return None

    env = dict(os.environ)
    existing = [item for item in env.get("LD_LIBRARY_PATH", "").split(os.pathsep) if item]
    merged: List[str] = []
    for item in library_dirs + existing:
        if item not in merged:
            merged.append(item)
    env["LD_LIBRARY_PATH"] = os.pathsep.join(merged)
    return env


def _browser_version(browser) -> str:
    version_attr = getattr(browser, "version", "")
    try:
        raw_version = version_attr() if callable(version_attr) else version_attr
    except Exception as exc:
        logger.debug("Failed to read browser version: %s", exc)
        raw_version = ""
    match = re.search(r"\d+(?:\.\d+){0,3}", str(raw_version or ""))
    return match.group(0) if match else "136.0.0.0"


def _browser_user_agent(browser) -> str:
    override = (os.getenv("MONITOR_WEB_USER_AGENT") or "").strip()
    if override:
        return override
    return (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{_browser_version(browser)} Safari/537.36"
    )


def _dismiss_common_overlays(page: Page) -> None:
    for selector in _OVERLAY_DISMISS_SELECTORS:
        try:
            target = page.locator(selector).first
            if target.count() == 0 or not target.is_visible():
                continue
            target.click(timeout=2000)
            logger.info("Dismissed page overlay using selector '%s'.", selector)
            time.sleep(0.4)
            _update_activity()
        except Exception as exc:
            logger.debug("Overlay dismiss failed for '%s': %s", selector, exc)


def _preferred_us_zip() -> str:
    raw = re.sub(r"\D+", "", (os.getenv("MONITOR_WEB_US_ZIP") or "").strip())
    if len(raw) >= 5:
        return raw[:5]
    return _DEFAULT_US_ZIP


def _is_amazon_dot_com_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("amazon.com")


def _normalize_delivery_text(text: str | None) -> str:
    compact = re.sub(r"\s+", " ", (text or "").replace("\u200c", " ").strip())
    compact = re.sub(r"[^0-9A-Za-z ]+", " ", compact)
    return re.sub(r"\s+", " ", compact).strip().lower()


def _amazon_delivery_location(page: Page) -> str:
    selectors = (
        "#glow-ingress-line2",
        "#glow-ingress-block",
    )
    for selector in selectors:
        try:
            target = page.locator(selector).first
            if target.count() == 0:
                continue
            text = (target.inner_text(timeout=2000) or "").strip()
            if text:
                return text
        except Exception as exc:
            logger.debug("Failed to read Amazon delivery location from %s: %s", selector, exc)
    return ""


def _extract_glow_validation_token(page: Page) -> str:
    candidates = (
        ("#glowValidationToken", "value"),
        ("input[name='glow-validation-token']", "value"),
        ("meta[name='anti-csrftoken-a2z']", "content"),
    )
    for selector, attribute in candidates:
        try:
            target = page.locator(selector).first
            if target.count() == 0:
                continue
            value = (target.get_attribute(attribute) or "").strip()
            if value:
                return value
        except Exception as exc:
            logger.debug("Failed to extract Amazon glow token from %s: %s", selector, exc)
    return ""


def _request_amazon_delivery_change(page: Page, token: str, desired_zip: str) -> dict | None:
    """向 Amazon 发送 AJAX 请求，将配送地址设置为指定邮编。

    Returns:
        API 响应字典（含 ok/status/data/raw），失败时返回 None。
    """
    try:
        return page.evaluate(
            """
            async ({ token, zipCode }) => {
                const body = new URLSearchParams({
                    locationType: 'LOCATION_INPUT',
                    zipCode,
                }).toString();
                const resp = await fetch('/gp/delivery/ajax/address-change.html', {
                    method: 'POST',
                    headers: {
                        'anti-csrftoken-a2z': token,
                        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'x-requested-with': 'XMLHttpRequest',
                    },
                    body,
                });
                const raw = await resp.text();
                let data = null;
                try {
                    data = JSON.parse(raw);
                } catch (err) {
                    data = null;
                }
                return {
                    ok: resp.ok,
                    status: resp.status,
                    data,
                    raw: raw.slice(0, 500),
                };
            }
            """,
            {"token": token, "zipCode": desired_zip},
        )
    except Exception as exc:
        logger.warning("Amazon delivery location update request failed: %s", exc)
        return None


def _reload_after_delivery_update(page: Page, task_url: str, desired_zip: str) -> None:
    """配送地址更新成功后，重新加载页面并记录结果。"""
    _update_activity()
    time.sleep(random.uniform(0.5, 1.0))
    try:
        page.reload(wait_until="domcontentloaded", timeout=45000)
    except Exception as exc:
        logger.debug("Amazon delivery reload failed, falling back to current URL navigation: %s", exc)
        if not _navigate_with_retry(page, page.url or task_url):
            logger.warning("Amazon delivery location updated but page reload/navigation failed.")
            return

    _dismiss_common_overlays(page)
    updated_location = _amazon_delivery_location(page)
    logger.info(
        "Amazon delivery location updated successfully to '%s'.",
        updated_location or desired_zip,
    )


def _ensure_amazon_us_delivery(page: Page, task_url: str) -> bool:
    if not _is_amazon_dot_com_url(task_url):
        return False

    desired_zip = _preferred_us_zip()
    current_location = _amazon_delivery_location(page)
    if desired_zip in _normalize_delivery_text(current_location):
        return False

    token = _extract_glow_validation_token(page)
    if not token:
        logger.debug("Amazon delivery location update skipped: glow validation token not found.")
        return False

    logger.info(
        "Updating Amazon delivery location to US zip %s (current='%s').",
        desired_zip,
        current_location or "unknown",
    )

    response = _request_amazon_delivery_change(page, token, desired_zip)
    if response is None:
        return False

    data = response.get("data") or {}
    if not response.get("ok") or not data.get("successful") or not data.get("isValidAddress"):
        logger.warning(
            "Amazon delivery location update was rejected (status=%s, response=%s).",
            response.get("status"),
            response.get("raw"),
        )
        return False

    _reload_after_delivery_update(page, task_url, desired_zip)
    return True


def _is_blocked(page: Page) -> bool:
    try:
        title = (page.title() or "").lower()
    except Exception as exc:
        logger.debug("Failed to read page title for block check: %s", exc)
        title = ""
    try:
        url = (page.url or "").lower()
    except Exception as exc:
        logger.debug("Failed to read page url for block check: %s", exc)
        url = ""
    try:
        body_text = (page.inner_text("body") or "")[:3000].lower()
    except Exception as exc:
        logger.debug("Failed to read body text for block check: %s", exc)
        body_text = ""
    if any(k in title or k in url or k in body_text for k in _BLOCK_KEYWORDS):
        return True
    try:
        if page.locator("input#captchacharacters, form[action*='validateCaptcha']").count() > 0:
            return True
    except Exception as exc:
        logger.debug("CAPTCHA check failed: %s", exc)
    return False


def _dump_page_snapshot(page: Page, label: str) -> None:
    """Dump HTML + screenshot for debugging when a scrape yields 0 products."""
    if os.getenv("MONITOR_WEB_DUMP_EMPTY", "").strip().lower() not in ("1", "true", "yes"):
        return
    log_dir = os.path.join(str(_runtime_base_dir()), "logs")
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.join(log_dir, f"{label}_{ts}")
    try:
        page.screenshot(path=base + ".png", full_page=True)
    except Exception as exc:
        logger.debug("Failed to dump screenshot: %s", exc)
    try:
        with open(base + ".html", "w", encoding="utf-8") as fh:
            fh.write(page.content())
    except Exception as exc:
        logger.debug("Failed to dump HTML: %s", exc)


def _normalize_url_identity(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def _extract_next_page_url(page: Page, base_url: str, task_url: str, page_num: int) -> str | None:
    selectors = [
        "a.s-pagination-next[href]",
        "a[aria-label='Go to next page'][href]",
        "a[aria-label*='Next'][href]",
        "li.a-last a[href]",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector)
            count = locator.count()
            if count == 0:
                continue
            for idx in range(count):
                href = (locator.nth(idx).get_attribute("href") or "").strip()
                if not href:
                    continue
                if "javascript:" in href.lower():
                    continue
                full_url = urljoin(base_url, href)
                if full_url:
                    return full_url
        except Exception as exc:
            logger.debug("Failed to extract next page url via '%s': %s", selector, exc)

    if _QUERY_PARAM_PAGINATION_ENABLED and page_num < _MAX_PAGES_DEFAULT:
        return _build_page_url(task_url, page_num + 1)
    return None


def _build_page_url(task_url: str, page_num: int) -> str:
    """Return the URL for a specific page by setting the 'page' query parameter."""
    parsed = urlparse(task_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["page"] = [str(page_num)]
    return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))


def _canonicalize_link(base_url: str, raw_link: str) -> str:
    if not raw_link:
        return ""
    link = urljoin(base_url, raw_link)
    parsed = urlparse(link)

    # Keep link stable for deduplication.
    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    # Canonical Amazon product URL: /dp/ASIN
    asin_match = AMAZON_ASIN_RE.search(clean)
    if asin_match:
        asin = asin_match.group(1).upper()
        return f"{parsed.scheme}://{parsed.netloc}/dp/{asin}"

    return clean.rstrip("/")


def _is_noise_link(link: str) -> bool:
    """Filter out known non-product or promo links."""
    if not link:
        return True
    lower = link.lower()
    if "/gift-cards/" in lower or "/giftcards/" in lower:
        return True
    if "/gc/" in lower:
        return True
    if "reload" in lower and "amazon.com" in lower:
        return True
    return False


def _extract_title(el) -> str:
    title_selectors = [
        "h2 a span",
        "h2 span",
        "h2 a",
        ".a-size-base-plus",
        ".a-size-medium",
        "[data-cy='title-recipe'] span",
        "img",
        "a[title]",
    ]
    for selector in title_selectors:
        try:
            node = el.locator(selector).first
            if node.count() == 0:
                continue
            if selector == "img":
                text = (node.get_attribute("alt") or "").strip()
            elif selector == "a[title]":
                text = (node.get_attribute("title") or "").strip()
            else:
                text = (node.inner_text() or "").strip()
            if len(text) >= 3:
                return text
        except Exception as exc:
            logger.debug("Failed to extract title: %s", exc)
            continue
    return ""


def _extract_link(el) -> str:
    href_selectors = [
        "a[href*='/dp/']",
        "a[href*='/gp/product/']",
        "h2 a",
        "a",
    ]
    for selector in href_selectors:
        try:
            node = el.locator(selector).first
            if node.count() == 0:
                continue
            href = (node.get_attribute("href") or "").strip()
            if href:
                return href
        except Exception as exc:
            logger.debug("Failed to extract link: %s", exc)
            continue
    return ""


def _scroll_to_load(page: Page, max_scroll_time: int = 60, click_show_more: bool = True) -> None:
    """Scroll the page until no more content loads.
    
    Args:
        page: Playwright page object.
        max_scroll_time: Maximum time in seconds to spend scrolling (safety limit).
        click_show_more: Whether to click "Show More" / "See More" buttons.
    """
    start_time = time.time()
    last_height = 0
    stable_count = 0
    
    # First, try to click any "Show More" / "See More" buttons
    if click_show_more:
        show_more_selectors = [
            "button:has-text('See more')",
            "button:has-text('Show more')",
            "a:has-text('See more')",
            "a:has-text('Show more')",
            "[data-action='show-more']",
            "button:has-text('Load more')",
        ]
        for selector in show_more_selectors:
            try:
                btn = page.locator(selector).first
                if btn.count() > 0 and btn.is_visible():
                    btn.click(timeout=2000)
                    logger.debug("Clicked 'Show More' button: %s", selector)
                    time.sleep(1.5)
                    _update_activity()
            except Exception:
                pass
    
    # Scroll until page height stops changing (all content loaded)
    scroll_count = 0
    while True:
        # Safety: check time limit
        if time.time() - start_time > max_scroll_time:
            logger.debug("Scroll timeout after %ds, stopping.", max_scroll_time)
            break
        
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2.0)  # Wait longer for lazy loading
        _update_activity()
        scroll_count += 1
        
        height = page.evaluate("document.body.scrollHeight")
        if height == last_height:
            stable_count += 1
            if stable_count >= 3:  # Height stable for 3 rounds = done loading
                logger.debug("Scroll complete after %d scrolls, height=%d", scroll_count, height)
                break
        else:
            stable_count = 0
        last_height = height
    
    page.evaluate("window.scrollTo(0, 0)")
    time.sleep(0.3)


def _get_elements(page: Page, selector: str):
    try:
        elements = page.locator(selector).all()
        if elements:
            return elements
    except Exception as exc:
        logger.warning("Primary selector '%s' failed, falling back: %s", selector, exc)

    # Fallbacks for Amazon pages where layouts can change.
    fallback_selectors = [
        "div.s-result-item[data-asin]:not([data-asin=''])",
        "div[data-component-type='s-search-result']",
        "div[data-asin]:not([data-asin=''])",
    ]
    for backup in fallback_selectors:
        try:
            elements = page.locator(backup).all()
            if elements:
                logger.info("Fallback selector '%s' matched %d elements", backup, len(elements))
                return elements
        except Exception as exc:
            logger.debug("Fallback selector '%s' failed: %s", backup, exc)

    return []


def _page_signal_counts(page: Page, selector: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    selectors: List[str] = []
    if selector:
        selectors.append(selector)
    selectors.extend(item for item in _RESULT_SIGNAL_SELECTORS if item != selector)

    for candidate in selectors:
        if candidate in counts:
            continue
        try:
            counts[candidate] = page.locator(candidate).count()
        except Exception as exc:
            logger.debug("Failed to count selector '%s': %s", candidate, exc)
            counts[candidate] = 0
    return counts


def _wait_for_result_signals(page: Page, selector: str, timeout_ms: int = _RESULT_WAIT_TIMEOUT_MS) -> bool:
    deadline = time.time() + (timeout_ms / 1000.0)
    while time.time() < deadline:
        _check_cancel()
        if _is_blocked(page):
            _dump_page_snapshot(page, "blocked_page")
            raise ScrapeTransientError("Amazon returned a block/captcha page while waiting for results.")
        counts = _page_signal_counts(page, selector)
        if any(counts.values()):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Detected product signals: %s", counts)
            return True
        _update_activity()
        time.sleep(1.0)
    return False


def _reload_for_recovery(page: Page) -> None:
    current_url = ""
    try:
        current_url = page.url or ""
    except Exception:
        current_url = ""

    try:
        page.reload(wait_until="domcontentloaded", timeout=90000)
        time.sleep(random.uniform(1.0, 2.0))
        _update_activity()
        return
    except Exception as exc:
        logger.warning("Page reload failed during scrape recovery: %s", exc)

    if not current_url:
        return
    if not _navigate_with_retry(page, current_url):
        raise ScrapeTransientError(f"Failed to recover page after empty scrape: {current_url}")


def _extract_asin(data_asin: str, link: str) -> str:
    """Return ASIN from a data-asin attribute or by parsing the URL."""
    if data_asin:
        return data_asin
    match = AMAZON_ASIN_RE.search(link)
    return match.group(1).upper() if match else ""


def _title_from_amazon_product_link(link: str) -> str:
    match = re.search(r"/([^/]+)/dp/[A-Z0-9]{10}", urlsplit(link).path, re.IGNORECASE)
    if not match:
        return ""
    slug = unquote(match.group(1))
    slug = slug.replace("-", " ").replace("_", " ")
    slug = re.sub(r"\s+", " ", slug).strip()
    return slug


def _collect_products_from_page(
    page: Page,
    selector: str,
    base_url: str,
    seen_asins: Set[str],
    seen_links: Set[str],
) -> List[Dict[str, str]]:
    products: List[Dict[str, str]] = []
    elements = _get_elements(page, selector)
    logger.info("Found %d product elements on current page", len(elements))

    for el in elements:
        try:
            data_asin = (el.get_attribute("data-asin") or "").strip().upper()
            raw_link = _extract_link(el)
            if not raw_link and data_asin:
                raw_link = f"/dp/{data_asin}"
            link = _canonicalize_link(base_url, raw_link)
            asin = _extract_asin(data_asin, link)

            # Deduplicate: prefer ASIN, fallback to link
            if asin:
                if asin in seen_asins:
                    continue
            else:
                if not link or link in seen_links:
                    continue

            title = _extract_title(el)
            if not title and asin:
                title = f"Amazon Product {asin}"
            if not title:
                continue

            # Filter out Amazon promotional cards and non-product noise.
            if _is_noise_title(title):
                logger.debug("Skipping noise product: %s", title)
                continue

            if asin:
                seen_asins.add(asin)
            if link:
                seen_links.add(link)
            products.append({"name": title, "link": link, "asin": asin})
            _add_partial_result({"name": title, "link": link, "asin": asin})  # Save to partial results.
            _update_activity()  # Mark progress when product captured.
        except Exception as exc:
            logger.debug("Element parse failed: %s", exc)

    # Last fallback: global anchors when element parsing gets empty.
    if not products:
        anchors = page.locator("a[href*='/dp/'],a[href*='/gp/product/']").all()
        for a in anchors:
            try:
                raw_link = (a.get_attribute("href") or "").strip()
                link = _canonicalize_link(base_url, raw_link)
                if _is_noise_link(link):
                    continue
                asin = _extract_asin("", link)
                if asin:
                    if asin in seen_asins:
                        continue
                elif not link or link in seen_links:
                    continue
                title = (a.inner_text() or "").strip()
                if len(title) < 3:
                    continue
                if _is_noise_title(title):
                    continue
                if asin:
                    seen_asins.add(asin)
                if link:
                    seen_links.add(link)
                products.append({"name": title, "link": link, "asin": asin})
                _add_partial_result({"name": title, "link": link, "asin": asin})  # Save to partial results.
                _update_activity()  # Mark progress when product captured.
            except Exception as exc:
                logger.debug("Fallback anchor parse failed: %s", exc)
                continue

    return products


def _navigate_with_retry(page: Page, url: str, max_attempts: int = 3) -> bool:
    for attempt in range(1, max_attempts + 1):
        _check_cancel()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            time.sleep(random.uniform(1.0, 2.2))
            _update_activity()  # Mark progress on successful navigation.
            return True
        except Exception as exc:
            logger.warning("Navigate failed attempt %d/%d: %s", attempt, max_attempts, exc)
            time.sleep(2.0 * attempt)
    return False


def _recover_empty_results_page(
    page: Page,
    selector: str,
    base_url: str,
    seen_asins: Set[str],
    seen_links: Set[str],
    page_label: str,
) -> List[Dict[str, str]]:
    if _ensure_amazon_us_delivery(page, page.url or base_url):
        _wait_for_result_signals(page, selector)
        _scroll_to_load(page, click_show_more=False)
        recovered = _collect_products_from_page(page, selector, base_url, seen_asins, seen_links)
        if recovered:
            logger.info("%s recovered after refreshing Amazon delivery location (%d products).", page_label, len(recovered))
            return recovered

    for attempt in range(1, _EMPTY_PAGE_RECOVERY_ATTEMPTS + 1):
        logger.warning(
            "%s returned 0 products; recovery attempt %d/%d.",
            page_label,
            attempt,
            _EMPTY_PAGE_RECOVERY_ATTEMPTS,
        )
        _reload_for_recovery(page)
        _wait_for_result_signals(page, selector)
        _scroll_to_load(page, click_show_more=False)
        recovered = _collect_products_from_page(page, selector, base_url, seen_asins, seen_links)
        if recovered:
            logger.info("%s recovered with %d products.", page_label, len(recovered))
            return recovered
    return []


def _scrape_all_pages(
    page: Page,
    selector: str,
    base_url: str,
    task_url: str,
    seen_asins: Set[str],
    seen_links: Set[str],
    max_pages: int = _MAX_PAGES_DEFAULT,
) -> List[Dict[str, str]]:
    products: List[Dict[str, str]] = []
    visited_page_urls: Set[str] = set()
    next_page_url: str | None = _normalize_url_identity(task_url)
    for page_num in range(1, max_pages + 1):
        _check_cancel()
        if page_num > 1:
            if not next_page_url:
                logger.info("Pagination ended after page %d - no next page link found.", page_num - 1)
                break
            target_url = _normalize_url_identity(next_page_url)
            if target_url in visited_page_urls:
                raise ScrapeTransientError(f"Pagination loop detected at page {page_num}: {target_url}")
            time.sleep(random.uniform(1.5, 3.0))
            if not _navigate_with_retry(page, next_page_url):
                raise ScrapeTransientError(f"Navigation to page {page_num} failed: {next_page_url}")
            if _is_blocked(page):
                raise ScrapeTransientError(f"Amazon blocked scrape on page {page_num}.")

        current_page_url = _normalize_url_identity(page.url or next_page_url or task_url)
        if current_page_url in visited_page_urls:
            raise ScrapeTransientError(f"Repeated page encountered during pagination: {current_page_url}")
        visited_page_urls.add(current_page_url)

        if not _wait_for_result_signals(page, selector):
            logger.info(
                "Timed out waiting for page %d result selectors; continuing with DOM fallback.",
                page_num,
            )

        _scroll_to_load(page, click_show_more=False)  # Don't click buttons that may navigate away
        page_products = _collect_products_from_page(page, selector, base_url, seen_asins, seen_links)
        if not page_products:
            page_products = _recover_empty_results_page(
                page,
                selector,
                base_url,
                seen_asins,
                seen_links,
                f"Search page {page_num}",
            )
        products.extend(page_products)
        logger.info(
            "Page %d collected %d products, running total=%d",
            page_num, len(page_products), len(products),
        )

        if not page_products:
            if page_num == 1:
                try:
                    logger.warning(
                        "Empty results on page 1. title=%s url=%s",
                        page.title(),
                        page.url,
                    )
                except Exception as exc:
                    logger.debug("Failed to get page title/url: %s", exc)
                    logger.warning("Empty results on page 1.")
                _dump_page_snapshot(page, "empty_page")
                raise ScrapeTransientError("Search landing page produced 0 products after recovery.")
            logger.info("No products on page %d, stopping pagination.", page_num)
            break

        next_page_url = _extract_next_page_url(page, base_url, task_url, page_num)
        if next_page_url:
            next_page_url = _normalize_url_identity(next_page_url)

    return products


def _product_matches(db_product: ProductItem, item: Dict[str, str]) -> bool:
    """Check whether a scraped item matches a DB record — prefer ASIN, fallback to link."""
    item_asin = item.get("asin", "")
    if item_asin and db_product.asin:
        return item_asin == db_product.asin
    return item.get("link", "") == db_product.product_link


def _required_integrity_ratio(reference_count: int) -> float:
    if reference_count >= 100:
        return _INTEGRITY_RATIO_LARGE
    if reference_count >= 20:
        return _INTEGRITY_RATIO_MEDIUM
    return _INTEGRITY_RATIO_SMALL


def _dedupe_scraped_products(current_products: List[Dict[str, str]]) -> List[Dict[str, str]]:
    deduped: List[Dict[str, str]] = []
    seen_keys: Set[str] = set()
    for item in current_products:
        name = (item.get("name") or "").strip()
        link = (item.get("link") or "").strip()
        asin = (item.get("asin") or "").strip().upper()
        if not name or not link:
            continue
        key = asin or link
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append({"name": name, "link": link, "asin": asin})
    return deduped


class ScrapeIncomplete(Exception):
    """Raised when scrape returned too few products (network issue suspected)."""
    pass


def _sync_products_to_db(
    db: Session,
    task_id: int,
    current_products: List[Dict[str, str]],
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Atomic sync of scraped products to database.
    
    Implements:
    1. Atomic operation: all changes commit together or rollback on failure
    2. Integrity check: reject low-confidence scrapes based on historical reference count
    3. Removal confirmation: products must be missing across multiple consecutive scrapes
    4. Peak tracking: remember historical max product count for integrity validation
    
    Returns: (current_products, new_products, removed_products)
    Raises: ScrapeIncomplete if scrape returned too few products (should retry later)
    """
    MISS_THRESHOLD = _REMOVAL_MISS_THRESHOLD
    MIN_PRODUCTS_THRESHOLD = _MIN_BASELINE_PRODUCTS
    
    now = datetime.datetime.now(datetime.timezone.utc)
    
    try:
        # Load task and all existing products
        from models import MonitorTask
        task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
        if not task:
            logger.error("Task %s not found in database.", task_id)
            return [], [], []
        
        current_products = _dedupe_scraped_products(current_products)
        all_db_products = db.query(ProductItem).filter(ProductItem.task_id == task_id).all()
        
        # BASELINE MODE: First successful crawl seeds data but does not alert
        if not all_db_products and current_products:
            # But reject if too few products on first run (likely network issue)
            if len(current_products) < MIN_PRODUCTS_THRESHOLD:
                logger.warning(
                    "[ATOMIC] Task %s: First run got only %d products - likely network issue. "
                    "Rejecting to avoid bad baseline.",
                    task_id, len(current_products)
                )
                raise ScrapeIncomplete(f"First run got only {len(current_products)} products")
            
            for item in current_products:
                if _is_noise_title(item.get("name")):
                    continue
                db.add(ProductItem(
                    task_id=task_id,
                    product_link=item["link"],
                    asin=item.get("asin") or None,
                    name=item["name"],
                    miss_count=0,
                ))
            # Set initial peak
            task.peak_product_count = len(current_products)
            db.commit()
            _clear_catalog_shift_state(task_id)
            logger.info(
                "Task %s baseline initialized with %d products; skip email on first run.",
                task_id, len(current_products),
            )
            return current_products, [], []
        
        # EMPTY SCRAPE: no-op at DB layer; scheduler decides whether to alert/retry.
        if not current_products:
            logger.warning("[ATOMIC] Task %s finished with 0 products - skip DB sync.", task_id)
            return [], [], []
        
        # INTEGRITY CHECK: Reject incomplete scrapes
        peak = task.peak_product_count or 0
        active_in_db = sum(1 for p in all_db_products if p.removed_at is None)
        reference_count = max(peak, active_in_db)
        
        if reference_count >= _INTEGRITY_MIN_REFERENCE_COUNT:
            integrity_ratio = _required_integrity_ratio(reference_count)
            scrape_ratio = len(current_products) / reference_count
            if scrape_ratio < integrity_ratio:
                logger.warning(
                    "[ATOMIC] Task %s: Scrape incomplete - got %d products, expected ~%d (ratio=%.2f < %.2f). "
                    "REJECTING this scrape - will retry later.",
                    task_id, len(current_products), reference_count, scrape_ratio, integrity_ratio
                )
                # Raise exception so scheduler knows to retry
                raise ScrapeIncomplete(
                    f"Got {len(current_products)} products, expected ~{reference_count} (ratio={scrape_ratio:.2f})"
                )
        
        # Update peak if we got more products than ever before
        if len(current_products) > peak:
            task.peak_product_count = len(current_products)
            logger.info("Task %s: New peak product count: %d (was %d)", task_id, len(current_products), peak)
        
        # Clean noise products from DB
        noise_ids = {p.id for p in all_db_products if _is_noise_title(p.name)}
        if noise_ids:
            for p in all_db_products:
                if p.id in noise_ids:
                    db.delete(p)
            all_db_products = [p for p in all_db_products if p.id not in noise_ids]
            logger.info("Task %s: removed %d noise products from DB.", task_id, len(noise_ids))
        
        # Filter noise from current scrape
        current_products = [item for item in current_products if not _is_noise_title(item.get("name"))]
        current_products = _dedupe_scraped_products(current_products)
        active_db_products = [p for p in all_db_products if p.removed_at is None]

        confirmation_reason = _pending_catalog_shift_confirmation(
            task_id,
            reference_count,
            active_db_products,
            current_products,
        )
        if confirmation_reason:
            logger.warning("[ATOMIC] Task %s: %s. REJECTING this scrape - will retry later.", task_id, confirmation_reason)
            raise ScrapeIncomplete(confirmation_reason)

        # Build lookup sets
        db_asins = {p.asin for p in all_db_products if p.asin}
        db_links = {p.product_link for p in all_db_products}
        current_asins = {item["asin"] for item in current_products if item.get("asin")}
        current_links = {item["link"] for item in current_products}
        
        # === DETECT NEW PRODUCTS ===
        new_products: List[Dict[str, str]] = []
        for item in current_products:
            asin = item.get("asin", "")
            is_known = (asin and asin in db_asins) or item["link"] in db_links
            if not is_known:
                new_products.append(item)
                db.add(ProductItem(
                    task_id=task_id,
                    product_link=item["link"],
                    asin=asin or None,
                    name=item["name"],
                    miss_count=0,
                ))
        
        removed_products: List[Dict[str, str]] = []
        if _TRACK_REMOVALS:
            # Optional legacy behavior: confirm removals after repeated misses.
            for p in all_db_products:
                if p.removed_at is not None:
                    # Already marked as removed, skip
                    continue

                still_present = (
                    (p.asin and p.asin in current_asins)
                    or p.product_link in current_links
                )

                if still_present:
                    # Product found - reset miss count
                    if p.miss_count > 0:
                        logger.info(
                            "Task %s: Product %s found again, resetting miss_count from %d to 0.",
                            task_id,
                            p.asin or p.product_link[:50],
                            p.miss_count,
                        )
                    p.miss_count = 0
                else:
                    # Product not found - increment miss count
                    p.miss_count = (p.miss_count or 0) + 1
                    logger.warning(
                        "[ATOMIC] Task %s: Product %s not found (miss_count=%d/%d)",
                        task_id,
                        p.asin or p.product_link[:50],
                        p.miss_count,
                        MISS_THRESHOLD,
                    )

                    if p.miss_count >= MISS_THRESHOLD:
                        # Confirmed removal after multiple consecutive misses
                        p.removed_at = now
                        removed_products.append({"name": p.name, "link": p.product_link})
                        logger.warning(
                            "[ATOMIC] Task %s: Product %s CONFIRMED REMOVED after %d consecutive misses.",
                            task_id,
                            p.asin or p.product_link[:50],
                            MISS_THRESHOLD,
                        )
        else:
            for p in all_db_products:
                if p.miss_count:
                    p.miss_count = 0

        # === DETECT RESTORED PRODUCTS ===
        for p in all_db_products:
            if p.removed_at is None:
                continue
            is_back = (
                (p.asin and p.asin in current_asins)
                or p.product_link in current_links
            )
            if is_back:
                p.removed_at = None
                p.miss_count = 0
                logger.info("Task %s: product restored: %s", task_id, p.asin or p.product_link)
        
        # Back-fill ASIN for older DB rows
        _backfill_asins(all_db_products, current_products)
        
        # ATOMIC COMMIT - all changes at once
        db.commit()
        
        total_in_db = db.query(ProductItem).filter(
            ProductItem.task_id == task_id,
            ProductItem.removed_at.is_(None)
        ).count()
        
        logger.warning(
            "[ATOMIC] Task %s sync COMPLETE: scraped=%d, new=%d, removed=%d, total_in_db=%d, peak=%d",
            task_id, len(current_products), len(new_products), len(removed_products), total_in_db,
            task.peak_product_count or 0,
        )
        return current_products, new_products, removed_products
        
    except Exception as exc:
        # ATOMIC ROLLBACK - revert all changes on any error
        db.rollback()
        logger.error(
            "[ATOMIC] Task %s sync FAILED - rolled back all changes: %s",
            task_id, exc
        )
        raise


def _backfill_asins(
    db_products: List[ProductItem],
    current_products: List[Dict[str, str]],
) -> None:
    """Fill in missing ASIN values on existing DB rows using current scrape data."""
    link_to_asin = {
        item["link"]: item["asin"]
        for item in current_products
        if item.get("asin")
    }
    for p in db_products:
        if not p.asin and p.product_link in link_to_asin:
            p.asin = link_to_asin[p.product_link]


_SCRAPE_TIMEOUT = 7200  # Max seconds for a single browser scrape (2 hours).
_ACTIVITY_TIMEOUT = 180  # Max seconds without any progress (new product captured).

def _update_activity():
    """Update the last activity timestamp when progress is made."""
    runtime = _current_scrape_runtime()
    if runtime is not None:
        runtime.touch()


def _add_partial_result(product: Dict[str, str]):
    """Add a product to partial results (thread-safe)."""
    runtime = _current_scrape_runtime()
    if runtime is not None:
        runtime.add_partial(product)


def _get_partial_results(runtime: _ScrapeRuntime | None = None) -> List[Dict[str, str]]:
    """Get a copy of partial results."""
    active_runtime = runtime or _current_scrape_runtime()
    if active_runtime is None:
        return []
    return active_runtime.partial_results()


def _get_seconds_since_activity(runtime: _ScrapeRuntime | None = None) -> float:
    """Get seconds elapsed since last activity."""
    active_runtime = runtime or _current_scrape_runtime()
    if active_runtime is None:
        return 0.0
    return active_runtime.seconds_since_activity()


def fetch_products_for_task(db: Session, task_id: int) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if not task:
        logger.error("Task %s not found.", task_id)
        return [], [], []

    try:
        logger.info("Start scraping task=%s url=%s", task.id, task.url)
        current_products = _run_browser_scrape_with_timeout(task)
        return _sync_products_to_db(db, task_id, current_products)
    except ScrapeCancelled as exc:
        logger.warning("Scrape cancelled for task %s: %s", task_id, exc)
        raise
    except (ScrapeIncomplete, ScrapeTransientError):
        raise
    except Exception as exc:
        logger.exception("Scrape failed for task %s (%s): %s", task_id, task.url, exc)
        return [], [], []


def _run_browser_scrape_with_timeout(task: MonitorTask) -> List[Dict[str, str]]:
    """Run the browser scrape in a child thread with activity-based timeout.
    
    The timeout is based on time since last activity (product captured or page visited),
    not total elapsed time. This allows long scrapes for stores with many products.
    """
    result: List[Dict[str, str]] = []
    error: List[Exception] = []
    runtime = _ScrapeRuntime()

    def _target():
        _set_scrape_runtime(runtime)
        try:
            result.extend(_run_browser_scrape(task))
        except Exception as exc:
            error.append(exc)
        finally:
            _clear_scrape_runtime()

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    
    # Poll for completion with activity-based timeout.
    start_time = time.time()
    while t.is_alive():
        t.join(timeout=5.0)  # Check every 5 seconds.
        if not t.is_alive():
            break
        
        elapsed = time.time() - start_time
        idle_time = _get_seconds_since_activity(runtime)
        
        # Timeout if no activity for _ACTIVITY_TIMEOUT seconds.
        if idle_time > _ACTIVITY_TIMEOUT:
            partial = _get_partial_results(runtime)
            logger.error(
                "Task %s scrape timed out: no activity for %.0fs (total elapsed: %.0fs). Returning %d partial products.",
                task.id, idle_time, elapsed, len(partial)
            )
            request_cancel_scrape()
            _kill_zombie_browsers()
            t.join(timeout=10.0)
            if t.is_alive():
                logger.warning("Task %s scrape thread still alive after idle-timeout cleanup.", task.id)
            clear_cancel_scrape()
            return partial  # Return partial results if any.
        
        # Hard limit as fallback (e.g., 10x the activity timeout).
        if elapsed > _SCRAPE_TIMEOUT:
            partial = _get_partial_results(runtime)
            logger.warning(
                "Task %s reached hard timeout of %ds with %d products captured.",
                task.id, _SCRAPE_TIMEOUT, len(partial)
            )
            request_cancel_scrape()
            _kill_zombie_browsers()
            t.join(timeout=10.0)
            if t.is_alive():
                logger.warning("Task %s scrape thread still alive after hard-timeout cleanup.", task.id)
            clear_cancel_scrape()
            return partial  # Return partial results.

    if error:
        raise error[0]
    return result


def _kill_zombie_browsers():
    """Best-effort cleanup of orphaned Chromium processes spawned by Playwright."""
    try:
        import subprocess
        commands = []
        if os.name == "nt":
            commands = [
                ["taskkill", "/F", "/IM", "chromium.exe", "/T"],
                ["taskkill", "/F", "/IM", "chrome.exe", "/T"],
            ]
        else:
            commands = [
                ["pkill", "-f", "playwright.*chromium"],
                ["pkill", "-f", "chromium.*--remote-debugging-pipe"],
            ]

        for command in commands:
            try:
                subprocess.run(command, capture_output=True, timeout=10, check=False)
            except FileNotFoundError:
                logger.debug("Browser cleanup command not available: %s", command[0])
    except Exception as exc:
        logger.debug("Failed to kill zombie browsers: %s", exc)


def _create_browser_context(task: MonitorTask):
    """Shared helper: launch browser, create context and page, navigate to task URL.

    Returns (playwright, browser, context, page, base_url) on success.
    Raises ScrapeTransientError on navigation failure or bot-block detection.
    """
    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    browser = None
    context = None
    try:
        browser = _launch_browser(pw)
        storage_state = _fresh_storage_state(task.url)
        context_kwargs: dict = {
            "user_agent": _browser_user_agent(browser),
            "viewport": {"width": 1366, "height": 900},
            "locale": "en-US",
            "timezone_id": os.getenv("MONITOR_WEB_TZ", "America/Los_Angeles"),
            "extra_http_headers": {
                "Accept-Language": "en-US,en;q=0.9",
                "Upgrade-Insecure-Requests": "1",
                "DNT": "1",
            },
        }
        proxy_server = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or ""
        if proxy_server:
            context_kwargs["proxy"] = {"server": proxy_server}
            _parsed_proxy = urlparse(proxy_server)
            _safe_proxy = (
                proxy_server
                if not _parsed_proxy.username
                else f"{_parsed_proxy.scheme}://{_parsed_proxy.hostname}"
                + (f":{_parsed_proxy.port}" if _parsed_proxy.port else "")
                + (_parsed_proxy.path or "")
            )
            logger.info("Using proxy: %s", _safe_proxy)
        if storage_state:
            context_kwargs["storage_state"] = storage_state
            logger.info("Reusing Playwright storage state: %s", storage_state)

        context = browser.new_context(**context_kwargs)
        # Reduce basic automation fingerprints.
        context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = window.chrome || { runtime: {} };
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            """
        )
        page = context.new_page()
        page.on("dialog", lambda dialog: dialog.dismiss())
        page.set_default_timeout(45000)
        page.set_default_navigation_timeout(90000)

        parsed = urlparse(task.url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        # Pre-warm homepage to set cookies before hitting target.
        logger.debug("Pre-warming homepage: %s", base_url)
        try:
            page.goto(base_url, wait_until="domcontentloaded", timeout=45000)
            _dismiss_common_overlays(page)
            _ensure_amazon_us_delivery(page, task.url)
            time.sleep(random.uniform(0.8, 1.6))
            logger.debug("Homepage pre-warm completed.")
        except Exception as exc:
            logger.debug("Pre-warm navigation failed: %s", exc)

        logger.debug("Navigating to target URL: %s", task.url)
        if not _navigate_with_retry(page, task.url):
            _clear_storage_state(task.url)
            raise ScrapeTransientError(f"Navigation failed after retries: {task.url}")

        _dismiss_common_overlays(page)
        if _ensure_amazon_us_delivery(page, task.url):
            logger.info("Revalidated Amazon delivery location on target page for task %s.", task.id)
        if _is_blocked(page):
            _dump_page_snapshot(page, "blocked_page")
            _clear_storage_state(task.url)
            raise ScrapeTransientError("Blocked by anti-bot page (captcha/robot check).")

        _save_storage_state(context, task.url)
        return pw, browser, context, page, base_url
    except Exception:
        if context is not None:
            try:
                context.close()
            except Exception:
                pass
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass
        pw.stop()
        raise


def _run_browser_scrape(task: MonitorTask) -> List[Dict[str, str]]:
    _check_cancel()
    if getattr(task, "task_type", "search") == "storefront":
        return _run_storefront_scrape(task)
    return _run_search_scrape(task)


def _run_search_scrape(task: MonitorTask) -> List[Dict[str, str]]:
    """Original search-results scraping logic."""
    seen_asins: Set[str] = set()
    seen_links: Set[str] = set()
    for attempt in range(1, _MAX_CONTEXT_ATTEMPTS + 1):
        _check_cancel()
        pw = browser = context = None
        persist_state = True
        try:
            pw, browser, context, page, base_url = _create_browser_context(task)
            return _scrape_all_pages(page, task.selector, base_url, task.url, seen_asins, seen_links)
        except ScrapeTransientError as exc:
            persist_state = False
            logger.warning("Search scrape attempt %d/%d failed: %s", attempt, _MAX_CONTEXT_ATTEMPTS, exc)
            if attempt < _MAX_CONTEXT_ATTEMPTS:
                time.sleep(2.0 * attempt)
                continue
            raise
        finally:
            if context is not None and persist_state:
                _save_storage_state(context, task.url)
            if browser is not None:
                try:
                    browser.close()
                except Exception as exc:
                    logger.debug("Failed to close browser cleanly: %s", exc)
            if pw is not None:
                try:
                    pw.stop()
                except Exception as exc:
                    logger.debug("Failed to stop Playwright cleanly: %s", exc)
    return []


# ---------------------------------------------------------------------------
# Amazon Storefront scraping
# ---------------------------------------------------------------------------

_STOREFRONT_NAV_SELECTORS = [
    "a[href*='/stores/page/']",
    "a[href*='/stores/']",
    "ul.stores-tab-list a",
    "div[class*='Tab'] a[href*='/stores/']",
]

_STOREFRONT_ASIN_BLOCK_RE = re.compile(r'"asin":"([A-Z0-9]{10})"', re.IGNORECASE)
_STOREFRONT_PAGE_URL_RE = re.compile(r"(/stores/page/[A-Z0-9-]{36}(?:\?[^\"'<>\s#]*)?)", re.IGNORECASE)
_STOREFRONT_DROP_QUERY_KEYS = {
    "ingress",
    "visitid",
    "ref",
    "ref_",
    "tag",
    "linkcode",
    "creative",
    "creativeasin",
}


def _normalize_storefront_tab_url(url: str) -> str:
    """Normalize storefront tab URL while dropping Amazon tracking parameters."""
    parsed = urlsplit(url)
    path = parsed.path.rstrip("/") or "/"
    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in _STOREFRONT_DROP_QUERY_KEYS
    ]
    query = urlencode(query_pairs, doseq=True)
    return urlunsplit((parsed.scheme, parsed.netloc.lower(), path, query, ""))


def _discover_storefront_tabs_from_html(html_content: str, base_url: str) -> List[str]:
    tab_urls: List[str] = []
    seen: Set[str] = set()
    if not html_content:
        return tab_urls

    normalized_html = html_content.replace("\\/", "/")
    for match in _STOREFRONT_PAGE_URL_RE.finditer(normalized_html):
        full_url = urljoin(base_url, html.unescape(match.group(1)))
        canonical = _normalize_storefront_tab_url(full_url)
        if canonical in seen:
            continue
        seen.add(canonical)
        tab_urls.append(full_url)
    return tab_urls


def _expand_storefront_menus(page: Page, nav_scope: Locator | None = None) -> None:
    """Best-effort hover to reveal dropdown menu links on storefront pages."""
    scope = nav_scope or page
    # Target dropdown triggers in Amazon storefront navigation
    dropdown_selectors = [
        # Amazon storefront specific selectors
        "button[aria-haspopup='true']",
        "a[aria-haspopup='true']",
        "[role='button'][aria-expanded]",
        # Navigation items with dropdowns (like "DEALS ▼", "LIVING ROOM ▼")
        "nav a:has-text('▼')",
        "nav button:has-text('▼')",
        # Generic dropdown triggers
        "[aria-haspopup='true']",
        "[aria-haspopup='menu']",
        "[data-action]",
    ]
    
    clicked_count = 0
    max_hovers = 15
    hovered_keys: Set[str] = set()
    
    logger.debug("Expanding dropdown menus...")
    
    for selector in dropdown_selectors:
        if clicked_count >= max_hovers:
            break
        try:
            locator = scope.locator(selector)
            count = locator.count()
            if count == 0:
                continue
            logger.debug("Found %d elements matching '%s'", count, selector)
            for idx in range(min(count, max_hovers - clicked_count)):
                try:
                    target = locator.nth(idx)
                    key = ""
                    try:
                        key = "|".join(
                            [
                                target.get_attribute("id") or "",
                                target.get_attribute("href") or "",
                                (target.inner_text(timeout=1000) or "").strip(),
                            ]
                        )
                    except Exception:
                        key = f"{selector}:{idx}"
                    if key in hovered_keys:
                        continue
                    hovered_keys.add(key)
                    # Only hover to reveal dropdown content - DO NOT click (would navigate away)
                    target.hover(timeout=2000)
                    time.sleep(0.5)  # Wait for dropdown to appear
                    _update_activity()
                    clicked_count += 1
                    logger.debug("Hovered dropdown %d", clicked_count)
                except Exception as exc:
                    logger.debug("Dropdown hover failed: %s", exc)
                    continue
        except Exception as exc:
            logger.debug("Dropdown selector '%s' failed: %s", selector, exc)
            continue
    
    logger.debug("Expanded %d dropdown menus via hover.", clicked_count)


def _storefront_nav_scope(page: Page) -> Locator | None:
    """Find the storefront navigation bar element."""
    # Try various selectors for Amazon storefront navigation
    nav_selectors = [
        # Standard nav elements
        "nav[aria-label*='Navigation Bar']",
        "nav[aria-label*='Store']",
        "nav[aria-label*='store']",
        # Amazon storefront specific - the bar with HOME, DEALS, etc.
        "div[class*='stores-tab']",
        "ul[class*='stores-tab']",
        # Generic navigation patterns
        "nav:has(a:has-text('HOME'))",
        "div:has(> a:has-text('HOME')):has(> a:has-text('NEW'))",
    ]
    for selector in nav_selectors:
        try:
            nav = page.locator(selector)
            if nav.count():
                logger.debug("Found nav scope with selector: %s", selector)
                return nav.first
        except Exception as exc:
            logger.debug("Failed to check nav element '%s': %s", selector, exc)
            continue
    return None


def _discover_storefront_tabs(page: Page, base_url: str) -> List[str]:
    """Find all sub-page links on an Amazon Storefront navigation bar."""
    seen: Set[str] = set()
    tab_urls: List[str] = []
    
    logger.debug("Looking for nav scope...")
    nav_scope = _storefront_nav_scope(page)
    logger.debug("Nav scope found: %s", nav_scope is not None)
    
    logger.debug("Expanding storefront menus...")
    _expand_storefront_menus(page, nav_scope)
    logger.debug("Menu expansion done.")

    for selector in _STOREFRONT_NAV_SELECTORS:
        scope = nav_scope or page
        logger.debug("Trying selector: %s", selector)
        try:
            # Use count() first to avoid slow .all() on large sets
            locator = scope.locator(selector)
            count = locator.count()
            logger.debug("Selector '%s' matched %d elements", selector, count)
            if count == 0:
                continue
            # Limit to first 50 elements to avoid timeout
            max_elements = min(count, 50)
            for idx in range(max_elements):
                _update_activity()  # Keep alive during discovery
                try:
                    a = locator.nth(idx)
                    href = (a.get_attribute("href") or "").strip()
                    if not href:
                        continue
                    full_url = urljoin(base_url, href)
                    # Only keep Amazon store page URLs.
                    if "/stores/" not in full_url:
                        continue
                    parsed = urlparse(full_url)
                    canonical = _normalize_storefront_tab_url(full_url)
                    if canonical not in seen:
                        seen.add(canonical)
                        tab_urls.append(full_url)
                except Exception as exc:
                    logger.debug("Storefront tab discovery failed: %s", exc)
                    continue
        except Exception as exc:
            logger.debug("Selector '%s' failed: %s", selector, exc)
            continue

    try:
        html_content = page.content()
    except Exception as exc:
        logger.debug("Storefront tab HTML read failed: %s", exc)
        html_content = ""
    for full_url in _discover_storefront_tabs_from_html(html_content, base_url):
        canonical = _normalize_storefront_tab_url(full_url)
        if canonical in seen:
            continue
        seen.add(canonical)
        tab_urls.append(full_url)

    logger.debug("Discovered %d storefront tab URLs.", len(tab_urls))
    return tab_urls


def _collect_asin_links_from_page(
    page: Page,
    base_url: str,
    seen_asins: Set[str],
) -> List[Dict[str, str]]:
    """Scan all links on the current page for /dp/ASIN patterns."""
    products: List[Dict[str, str]] = []
    anchors = page.locator("a[href*='/dp/'],a[href*='/gp/product/']").all()
    
    # First pass: count unique ASINs on this page
    page_asins: Set[str] = set()
    for a in anchors:
        try:
            href = (a.get_attribute("href") or "").strip()
            asin_match = AMAZON_ASIN_RE.search(href)
            if asin_match:
                page_asins.add(asin_match.group(1).upper())
        except Exception:
            pass
    
    # Count how many are duplicates from previous pages
    new_asins = page_asins - seen_asins
    duplicate_asins = page_asins & seen_asins
    logger.debug("Page has %d unique ASINs (%d new, %d already seen from other pages).", 
                   len(page_asins), len(new_asins), len(duplicate_asins))

    for a in anchors:
        try:
            href = (a.get_attribute("href") or "").strip()
            full_url = urljoin(base_url, href)
            link = _canonicalize_link(base_url, href)
            if _is_noise_link(link):
                logger.debug("Skipping noise link: %s", link)
                continue
            asin_match = AMAZON_ASIN_RE.search(link)
            if not asin_match:
                continue
            asin = asin_match.group(1).upper()
            if asin in seen_asins:
                continue

            # Try to get a title from the anchor or its children.
            title = ""
            try:
                title = (a.inner_text() or "").strip()
            except Exception as exc:
                logger.debug("Storefront anchor text read failed: %s", exc)
            if len(title) < 3:
                for attribute in ("title", "aria-label"):
                    try:
                        title = (a.get_attribute(attribute) or "").strip()
                    except Exception as exc:
                        logger.debug("Storefront anchor %s read failed: %s", attribute, exc)
                        title = ""
                    if len(title) >= 3:
                        break
            if len(title) < 3:
                # Fallback: check for an img alt inside or nearby.
                try:
                    img = a.locator("img").first
                    if img.count():
                        title = (img.get_attribute("alt") or "").strip()
                except Exception as exc:
                    logger.debug("Storefront anchor img alt read failed: %s", exc)
            if len(title) < 3:
                title = _title_from_amazon_product_link(full_url)
            if len(title) < 3:
                title = f"Amazon Product {asin}"

            if _is_noise_title(title):
                logger.debug("Skipping noise title: '%s' (ASIN=%s)", title, asin)
                continue

            seen_asins.add(asin)
            products.append({"name": title, "link": link, "asin": asin})
            _add_partial_result({"name": title, "link": link, "asin": asin})  # Save to partial results.
            _update_activity()  # Mark progress when product captured.
            logger.debug("Storefront product captured: %s (ASIN=%s) total=%d.", title, asin, len(products))
        except Exception as exc:
            logger.debug("Storefront anchor parse failed: %s", exc)

    try:
        html_content = page.content()
    except Exception as exc:
        logger.debug("Storefront HTML content read failed: %s", exc)
        html_content = ""
    if html_content:
        html_products = _collect_storefront_products_from_html(html_content, base_url, seen_asins)
        if html_products:
            logger.debug("Storefront HTML fallback recovered %d additional products.", len(html_products))
            products.extend(html_products)

    return products


def _collect_storefront_products_from_html(
    html_content: str,
    base_url: str,
    seen_asins: Set[str],
) -> List[Dict[str, str]]:
    products: List[Dict[str, str]] = []
    matches = list(_STOREFRONT_ASIN_BLOCK_RE.finditer(html_content))
    if not matches:
        return products

    max_block_span = 30000
    for idx, match in enumerate(matches):
        asin = match.group(1).upper()
        if asin in seen_asins:
            continue

        next_start = matches[idx + 1].start() if idx + 1 < len(matches) else len(html_content)
        chunk = html_content[match.start(): min(next_start, match.start() + max_block_span)]

        url_match = re.search(
            rf'"detailPageLinkURL":"(?P<url>[^"]*?/dp/{asin}[^"]*)"',
            chunk,
            re.IGNORECASE,
        )
        if not url_match:
            url_match = re.search(
                rf'href="(?P<url>[^"]*?/dp/{asin}[^"]*)"',
                chunk,
                re.IGNORECASE,
            )

        raw_link = html.unescape(url_match.group("url")) if url_match else f"/dp/{asin}"
        full_url = urljoin(base_url, raw_link)
        link = _canonicalize_link(base_url, raw_link)
        if _is_noise_link(link):
            continue

        title = ""
        for pattern in (
            r'"altText":"(?P<title>[^"]+)"',
            r'"title":"(?P<title>[^"]+)"',
            r'aria-label="(?P<title>[^"]+)"',
            r'title="(?P<title>[^"]+)"',
        ):
            title_match = re.search(pattern, chunk, re.IGNORECASE)
            if not title_match:
                continue
            candidate = html.unescape(title_match.group("title")).strip()
            if candidate:
                title = candidate
                break

        if len(title) < 3:
            title = _title_from_amazon_product_link(full_url)
        if len(title) < 3 or _is_noise_title(title):
            continue

        seen_asins.add(asin)
        product = {"name": title, "link": link, "asin": asin}
        products.append(product)
        _add_partial_result(product)
        _update_activity()

    return products


def _recover_empty_storefront_page(
    page: Page,
    base_url: str,
    seen_asins: Set[str],
    page_label: str,
) -> List[Dict[str, str]]:
    if _ensure_amazon_us_delivery(page, page.url or base_url):
        _wait_for_result_signals(page, "")
        _scroll_to_load(page, click_show_more=False)
        recovered = _collect_asin_links_from_page(page, base_url, seen_asins)
        if recovered:
            logger.info("%s recovered after refreshing Amazon delivery location (%d products).", page_label, len(recovered))
            return recovered

    for attempt in range(1, _EMPTY_PAGE_RECOVERY_ATTEMPTS + 1):
        logger.warning(
            "%s returned 0 products; recovery attempt %d/%d.",
            page_label,
            attempt,
            _EMPTY_PAGE_RECOVERY_ATTEMPTS,
        )
        _reload_for_recovery(page)
        _wait_for_result_signals(page, "")
        _scroll_to_load(page, click_show_more=False)
        recovered = _collect_asin_links_from_page(page, base_url, seen_asins)
        if recovered:
            logger.info("%s recovered with %d products.", page_label, len(recovered))
            return recovered
    return []


def _run_storefront_scrape(task: MonitorTask) -> List[Dict[str, str]]:
    """Scrape an Amazon Storefront by visiting each navigation tab."""
    seen_asins: Set[str] = set()
    logger.debug("Starting storefront scrape for task=%s url=%s", task.id, task.url)
    for attempt in range(1, _MAX_CONTEXT_ATTEMPTS + 1):
        _check_cancel()
        logger.debug("Storefront context attempt %d/%d", attempt, _MAX_CONTEXT_ATTEMPTS)
        pw = browser = context = None
        persist_state = True
        try:
            pw, browser, context, page, base_url = _create_browser_context(task)
            logger.debug("Browser context created successfully for task=%s", task.id)
            # Wait for page to properly load before scrolling
            logger.debug("Waiting for page content to load...")
            try:
                # Wait for body to have content
                page.wait_for_function("document.body.scrollHeight > 100", timeout=15000)
            except Exception as e:
                logger.debug("Page height check failed: %s. Current URL: %s", e, page.url)
                # Try to wait for any visible content
                try:
                    page.wait_for_selector("body *", timeout=10000)
                except Exception:
                    pass
            
            # Log current page state for debugging
            current_height = page.evaluate("document.body.scrollHeight")
            current_url = page.url
            logger.debug("Page state before scroll: height=%d, url=%s", current_height, current_url)
            
            # Collect products from the landing page first.
            logger.debug("Scrolling to load landing page content...")
            _wait_for_result_signals(page, "")
            _scroll_to_load(page, click_show_more=False)  # Don't click buttons that may navigate away
            products = _collect_asin_links_from_page(page, base_url, seen_asins)
            if not products:
                products = _recover_empty_storefront_page(
                    page,
                    base_url,
                    seen_asins,
                    "Storefront landing page",
                )
            logger.debug("Storefront landing: collected %d products.", len(products))

            # Discover and visit sub-pages.
            logger.debug("Discovering storefront tabs...")
            tab_urls = _discover_storefront_tabs(page, base_url)
            logger.debug("Discovered %d tab URLs.", len(tab_urls))
            current_url = page.url
            logger.debug("Will visit %d storefront tabs (current: %s)", len(tab_urls), current_url)
            for idx, tab_url in enumerate(tab_urls, 1):
                _check_cancel()
                # Skip if it's the same page we already scraped.
                if urlparse(tab_url).path == urlparse(current_url).path:
                    logger.debug("Skipping same page: %s", tab_url)
                    continue
                logger.debug("Visiting storefront tab %d/%d: %s", idx, len(tab_urls), tab_url)
                time.sleep(random.uniform(1.5, 3.0))
                _update_activity()  # Mark activity when visiting each tab (even if 0 new products)
                if not _navigate_with_retry(page, tab_url):
                    logger.warning("Storefront tab navigation failed: %s", tab_url)
                    continue
                _wait_for_result_signals(page, "")
                _scroll_to_load(page, click_show_more=False)  # Don't click buttons that may navigate away
                _update_activity()  # Mark activity after scroll completes
                tab_products = _collect_asin_links_from_page(page, base_url, seen_asins)
                if not tab_products:
                    tab_products = _recover_empty_storefront_page(
                        page,
                        base_url,
                        seen_asins,
                        f"Storefront tab {idx}",
                    )
                products.extend(tab_products)
                logger.debug("Tab %d/%d collected %d products, total=%d.",
                            idx, len(tab_urls), len(tab_products), len(products))

            if not products:
                raise ScrapeTransientError("Storefront scrape produced 0 products after recovery.")
            logger.debug("Storefront scrape complete: %d total products.", len(products))
            return products
        except ScrapeTransientError as exc:
            persist_state = False
            logger.warning("Storefront scrape attempt %d/%d failed: %s", attempt, _MAX_CONTEXT_ATTEMPTS, exc)
            if attempt < _MAX_CONTEXT_ATTEMPTS:
                time.sleep(2.0 * attempt)
                continue
            raise
        finally:
            if context is not None and persist_state:
                _save_storage_state(context, task.url)
            if browser is not None:
                try:
                    browser.close()
                except Exception as exc:
                    logger.debug("Failed to close browser cleanly: %s", exc)
            if pw is not None:
                try:
                    pw.stop()
                except Exception as exc:
                    logger.debug("Failed to stop Playwright cleanly: %s", exc)
    return []
