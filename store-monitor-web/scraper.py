from __future__ import annotations

import datetime
import logging
import os
import random
import re
import sys
import threading
import time
from typing import TYPE_CHECKING, Dict, List, Set, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse, urlsplit, urlunsplit

from sqlalchemy.orm import Session

from models import MonitorTask, ProductItem

if TYPE_CHECKING:  # only for type checkers; not imported at runtime
    from playwright.sync_api import Locator, Page

logger = logging.getLogger(__name__)

AMAZON_ASIN_RE = re.compile(r"/(?:dp|gp/product)/([A-Z0-9]{10})", re.IGNORECASE)
_MAX_CONTEXT_ATTEMPTS = int(os.getenv("MONITOR_WEB_NAV_ATTEMPTS", "3") or "3")
_MAX_PAGES_DEFAULT = int(os.getenv("MONITOR_WEB_MAX_PAGES", "20") or "20")
_cancel_event = threading.Event()

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
    "auto-reload",
    "sponsored",
)


class ScrapeCancelled(Exception):
    """Raised when a user explicitly cancels a running scrape."""
    pass


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


def _check_cancel() -> None:
    if _cancel_event.is_set():
        _cancel_event.clear()
        raise ScrapeCancelled("Scrape cancelled by user.")


def _launch_browser(playwright):
    headful = os.getenv("MONITOR_WEB_HEADFUL", "").strip().lower() in ("1", "true", "yes")
    try:
        return playwright.chromium.launch(
            headless=not headful,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
    except Exception as exc:
        logger.warning("Launch system Chrome failed, fallback to bundled Chromium: %s", exc)
        return playwright.chromium.launch(
            headless=not headful,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )


def _is_blocked(page: Page) -> bool:
    title = (page.title() or "").lower()
    url = (page.url or "").lower()
    body_text = (page.inner_text("body") or "")[:3000].lower()
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
    base_dir = os.path.dirname(os.path.abspath(sys.argv[0] if not getattr(sys, "frozen", False) else sys.executable))
    log_dir = os.path.join(base_dir, "logs")
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


def _scroll_to_load(page: Page, rounds: int = 8) -> None:
    last_height = 0
    stable_rounds = 0
    for _ in range(rounds):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1.2)
        height = page.evaluate("document.body.scrollHeight")
        if height == last_height:
            stable_rounds += 1
            if stable_rounds >= 2:
                break
        else:
            stable_rounds = 0
        last_height = height
    page.evaluate("window.scrollTo(0, 0)")
    time.sleep(0.3)


def _get_elements(page: Page, selector: str):
    elements = page.locator(selector).all()
    if elements:
        return elements

    # Fallbacks for Amazon pages where layouts can change.
    fallback_selectors = [
        "div.s-result-item[data-asin]:not([data-asin=''])",
        "div[data-component-type='s-search-result']",
        "div[data-asin]:not([data-asin=''])",
    ]
    for backup in fallback_selectors:
        elements = page.locator(backup).all()
        if elements:
            logger.info("Fallback selector '%s' matched %d elements", backup, len(elements))
            return elements

    return []


def _extract_asin(data_asin: str, link: str) -> str:
    """Return ASIN from a data-asin attribute or by parsing the URL."""
    if data_asin:
        return data_asin
    match = AMAZON_ASIN_RE.search(link)
    return match.group(1).upper() if match else ""


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
                seen_asins.add(asin)
            else:
                if not link or link in seen_links:
                    continue
            seen_links.add(link)

            title = _extract_title(el)
            if not title and asin:
                title = f"Amazon Product {asin}"
            if not title:
                continue

            # Filter out Amazon promotional cards and non-product noise.
            if _is_noise_title(title):
                logger.debug("Skipping noise product: %s", title)
                continue

            products.append({"name": title, "link": link, "asin": asin})
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
                    seen_asins.add(asin)
                elif not link or link in seen_links:
                    continue
                seen_links.add(link)
                title = (a.inner_text() or "").strip()
                if len(title) < 3:
                    continue
                if _is_noise_title(title):
                    continue
                products.append({"name": title, "link": link, "asin": asin})
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
            return True
        except Exception as exc:
            logger.warning("Navigate failed attempt %d/%d: %s", attempt, max_attempts, exc)
            time.sleep(2.0 * attempt)
    return False


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
    for page_num in range(1, max_pages + 1):
        _check_cancel()
        if page_num > 1:
            next_url = _build_page_url(task_url, page_num)
            time.sleep(random.uniform(1.5, 3.0))
            if not _navigate_with_retry(page, next_url):
                logger.warning("Navigation to page %d failed, stopping.", page_num)
                break
            if _is_blocked(page):
                logger.error("Blocked on page %d.", page_num)
                break

        try:
            page.wait_for_selector(selector, timeout=15000)
        except Exception:
            logger.info("Selector wait timeout on page %d, continuing with fallback.", page_num)

        _scroll_to_load(page)
        page_products = _collect_products_from_page(page, selector, base_url, seen_asins, seen_links)
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
                except Exception:
                    logger.warning("Empty results on page 1.")
                _dump_page_snapshot(page, "empty_page")
            logger.info("No products on page %d, stopping pagination.", page_num)
            break

    return products


def _product_matches(db_product: ProductItem, item: Dict[str, str]) -> bool:
    """Check whether a scraped item matches a DB record — prefer ASIN, fallback to link."""
    item_asin = item.get("asin", "")
    if item_asin and db_product.asin:
        return item_asin == db_product.asin
    return item.get("link", "") == db_product.product_link


def _sync_products_to_db(
    db: Session,
    task_id: int,
    current_products: List[Dict[str, str]],
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    now = datetime.datetime.now(datetime.timezone.utc)
    all_db_products = db.query(ProductItem).filter(ProductItem.task_id == task_id).all()

    # Baseline mode: first successful crawl seeds data but does not alert.
    if not all_db_products and current_products:
        for item in current_products:
            db.add(ProductItem(
                task_id=task_id,
                product_link=item["link"],
                asin=item.get("asin") or None,
                name=item["name"],
            ))
        db.commit()
        logger.info(
            "Task %s baseline initialized with %d products; skip email on first run.",
            task_id,
            len(current_products),
        )
        return current_products, [], []

    # If scrape returned nothing and existing data is present, treat as a failure —
    # don't mark every product as removed. The scheduler handles repeated empties separately.
    if not current_products:
        logger.warning("Task %s finished with 0 products.", task_id)
        return [], [], []

    noise_ids = {p.id for p in all_db_products if _is_noise_title(p.name)}
    if noise_ids:
        for p in all_db_products:
            if p.id in noise_ids:
                db.delete(p)
        all_db_products = [p for p in all_db_products if p.id not in noise_ids]
        logger.info("Task %s: removed %d noise products from DB.", task_id, len(noise_ids))

    # Build fast lookup sets from DB records.
    db_asins = {p.asin for p in all_db_products if p.asin}
    db_links = {p.product_link for p in all_db_products}

    # Build fast lookup sets from current scrape.
    current_asins = {item["asin"] for item in current_products if item.get("asin")}
    current_links = {item["link"] for item in current_products}

    # New products: in current scrape but never seen before (check ASIN first, then link).
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
            ))

    # Removed products: active in DB but absent from current scrape.
    removed_products: List[Dict[str, str]] = []
    for p in all_db_products:
        if p.removed_at is not None:
            continue
        still_present = (
            (p.asin and p.asin in current_asins)
            or p.product_link in current_links
        )
        if not still_present:
            p.removed_at = now
            removed_products.append({"name": p.name, "link": p.product_link})

    # Restored products: previously removed, now back in stock.
    for p in all_db_products:
        if p.removed_at is None:
            continue
        is_back = (
            (p.asin and p.asin in current_asins)
            or p.product_link in current_links
        )
        if is_back:
            p.removed_at = None
            logger.info("Task %s: product restored: %s", task_id, p.asin or p.product_link)

    # Back-fill ASIN for older DB rows that were stored without one.
    _backfill_asins(all_db_products, current_products)

    db.commit()

    logger.info(
        "Task %s done. current=%d, new=%d, removed=%d",
        task_id, len(current_products), len(new_products), len(removed_products),
    )
    return current_products, new_products, removed_products


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


_SCRAPE_TIMEOUT = 600  # Max seconds for a single browser scrape.


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
    except Exception as exc:
        logger.exception("Scrape failed for task %s (%s): %s", task_id, task.url, exc)
        return [], [], []


def _run_browser_scrape_with_timeout(task: MonitorTask) -> List[Dict[str, str]]:
    """Run the browser scrape in a child thread with a timeout to prevent infinite hangs."""
    result: List[Dict[str, str]] = []
    error: List[Exception] = []

    def _target():
        try:
            result.extend(_run_browser_scrape(task))
        except Exception as exc:
            error.append(exc)

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=_SCRAPE_TIMEOUT)

    if t.is_alive():
        logger.error("Task %s scrape timed out after %ds.", task.id, _SCRAPE_TIMEOUT)
        _kill_zombie_browsers()
        return []
    if error:
        raise error[0]
    return result


def _kill_zombie_browsers():
    """Best-effort cleanup of orphaned Chromium processes spawned by Playwright."""
    try:
        import subprocess
        subprocess.run(
            ["taskkill", "/F", "/IM", "chromium.exe", "/T"],
            capture_output=True, timeout=10,
        )
    except Exception as exc:
        logger.debug("Failed to kill zombie browsers: %s", exc)


def _create_browser_context(task: MonitorTask):
    """Shared helper: launch browser, create context and page, navigate to task URL.

    Returns (playwright, browser, page, base_url) on success.
    Raises RuntimeError on navigation failure or bot-block detection.
    """
    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    browser = _launch_browser(pw)
    context_kwargs: dict = {
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
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
    page.set_default_timeout(45000)
    page.set_default_navigation_timeout(90000)

    parsed = urlparse(task.url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    # Pre-warm homepage to set cookies before hitting target.
    try:
        page.goto(base_url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(random.uniform(0.8, 1.6))
    except Exception as exc:
        logger.debug("Pre-warm navigation failed: %s", exc)

    if not _navigate_with_retry(page, task.url):
        browser.close()
        pw.stop()
        raise RuntimeError(f"Navigation failed after retries: {task.url}")

    if _is_blocked(page):
        browser.close()
        pw.stop()
        raise RuntimeError("Blocked by anti-bot page (captcha/robot check).")

    return pw, browser, page, base_url


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
        try:
            pw, browser, page, base_url = _create_browser_context(task)
        except RuntimeError as exc:
            logger.error("%s", exc)
            if attempt < _MAX_CONTEXT_ATTEMPTS:
                time.sleep(2.0 * attempt)
                continue
            return []
        try:
            return _scrape_all_pages(page, task.selector, base_url, task.url, seen_asins, seen_links)
        finally:
            browser.close()
            pw.stop()
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


def _normalize_storefront_tab_url(url: str) -> str:
    """Normalize storefront tab URL while keeping query parameters."""
    parsed = urlsplit(url)
    path = parsed.path.rstrip("/") or "/"
    return urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, ""))


def _expand_storefront_menus(page: Page, nav_scope: Locator | None = None) -> None:
    """Best-effort hover to reveal dropdown menu links on storefront pages."""
    scope = nav_scope or page
    hover_selectors = [
        "[aria-haspopup='true']",
        "[role='menuitem']",
        "button",
        "li",
        "ul.stores-tab-list li",
    ]
    hovered = 0
    max_hovers = 25
    for selector in hover_selectors:
        if hovered >= max_hovers:
            break
        locator = scope.locator(selector)
        try:
            count = locator.count()
        except Exception as exc:
            logger.debug("Storefront hover discovery failed for %s: %s", selector, exc)
            continue
        for idx in range(min(count, max_hovers - hovered)):
            try:
                target = locator.nth(idx)
                target.hover()
                time.sleep(0.05)
                try:
                    target.click(timeout=500)
                except Exception:
                    pass
                hovered += 1
            except Exception as exc:
                logger.debug("Storefront hover failed: %s", exc)
                continue


def _storefront_nav_scope(page: Page) -> Locator | None:
    for selector in [
        "nav[aria-label*='Navigation Bar']",
        "nav[aria-label*='Store']",
        "nav[aria-label*='store']",
    ]:
        nav = page.locator(selector)
        try:
            if nav.count():
                return nav.first
        except Exception:
            continue
    return None


def _discover_storefront_tabs(page: Page, base_url: str) -> List[str]:
    """Find all sub-page links on an Amazon Storefront navigation bar."""
    seen: Set[str] = set()
    tab_urls: List[str] = []
    nav_scope = _storefront_nav_scope(page)
    _expand_storefront_menus(page, nav_scope)

    for selector in _STOREFRONT_NAV_SELECTORS:
        scope = nav_scope or page
        anchors = scope.locator(selector).all()
        if not anchors:
            continue
        for a in anchors:
            try:
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

    logger.info("Discovered %d storefront tab URLs.", len(tab_urls))
    return tab_urls


def _collect_asin_links_from_page(
    page: Page,
    base_url: str,
    seen_asins: Set[str],
) -> List[Dict[str, str]]:
    """Scan all links on the current page for /dp/ASIN patterns."""
    products: List[Dict[str, str]] = []
    anchors = page.locator("a[href*='/dp/'],a[href*='/gp/product/']").all()
    logger.info("Storefront page: found %d ASIN links.", len(anchors))

    for a in anchors:
        try:
            href = (a.get_attribute("href") or "").strip()
            link = _canonicalize_link(base_url, href)
            if _is_noise_link(link):
                continue
            asin_match = AMAZON_ASIN_RE.search(link)
            if not asin_match:
                continue
            asin = asin_match.group(1).upper()
            if asin in seen_asins:
                continue
            seen_asins.add(asin)

            # Try to get a title from the anchor or its children.
            title = ""
            try:
                title = (a.inner_text() or "").strip()
            except Exception as exc:
                logger.debug("Storefront anchor text read failed: %s", exc)
            if len(title) < 3:
                # Fallback: check for an img alt inside or nearby.
                try:
                    img = a.locator("img").first
                    if img.count():
                        title = (img.get_attribute("alt") or "").strip()
                except Exception as exc:
                    logger.debug("Storefront anchor img alt read failed: %s", exc)
            if len(title) < 3:
                title = f"Amazon Product {asin}"

            if _is_noise_title(title):
                continue

            products.append({"name": title, "link": link, "asin": asin})
            logger.info("Storefront product captured: %s (ASIN=%s) total=%d.", title, asin, len(seen_asins))
        except Exception as exc:
            logger.debug("Storefront anchor parse failed: %s", exc)

    return products


def _run_storefront_scrape(task: MonitorTask) -> List[Dict[str, str]]:
    """Scrape an Amazon Storefront by visiting each navigation tab."""
    seen_asins: Set[str] = set()
    for attempt in range(1, _MAX_CONTEXT_ATTEMPTS + 1):
        _check_cancel()
        try:
            pw, browser, page, base_url = _create_browser_context(task)
        except RuntimeError as exc:
            logger.error("%s", exc)
            if attempt < _MAX_CONTEXT_ATTEMPTS:
                time.sleep(2.0 * attempt)
                continue
            return []
        try:
            # Collect products from the landing page first.
            _scroll_to_load(page, rounds=10)
            products = _collect_asin_links_from_page(page, base_url, seen_asins)
            logger.info("Storefront landing: collected %d products.", len(products))

            # Discover and visit sub-pages.
            tab_urls = _discover_storefront_tabs(page, base_url)
            current_url = page.url
            for tab_url in tab_urls:
                _check_cancel()
                # Skip if it's the same page we already scraped.
                if urlparse(tab_url).path == urlparse(current_url).path:
                    continue
                time.sleep(random.uniform(1.5, 3.0))
                if not _navigate_with_retry(page, tab_url):
                    logger.warning("Storefront tab navigation failed: %s", tab_url)
                    continue
                if _is_blocked(page):
                    logger.error("Blocked on storefront tab: %s", tab_url)
                    break
                _scroll_to_load(page, rounds=10)
                tab_products = _collect_asin_links_from_page(page, base_url, seen_asins)
                products.extend(tab_products)
                logger.info("Storefront tab %s: collected %d products, total=%d.",
                            tab_url, len(tab_products), len(products))

            logger.info("Storefront scrape complete: %d total products.", len(products))
            return products
        finally:
            browser.close()
            pw.stop()
    return []
