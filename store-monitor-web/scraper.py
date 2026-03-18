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
    "reload your gift",
    "auto-reload",
    "sponsored",
    "amazon product b",  # Generic placeholder names like "Amazon Product B09XXX"
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
                    logger.warning("[DEBUG] Clicked 'Show More' button: %s", selector)
                    time.sleep(1.5)
                    _update_activity()
            except Exception:
                pass
    
    # Scroll until page height stops changing (all content loaded)
    scroll_count = 0
    while True:
        # Safety: check time limit
        if time.time() - start_time > max_scroll_time:
            logger.warning("[DEBUG] Scroll timeout after %ds, stopping.", max_scroll_time)
            break
        
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2.0)  # Wait longer for lazy loading
        _update_activity()
        scroll_count += 1
        
        height = page.evaluate("document.body.scrollHeight")
        if height == last_height:
            stable_count += 1
            if stable_count >= 3:  # Height stable for 3 rounds = done loading
                logger.warning("[DEBUG] Scroll complete after %d scrolls, height=%d", scroll_count, height)
                break
        else:
            stable_count = 0
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
        except Exception as exc:
            logger.debug("Selector wait timeout on page %d: %s", page_num, exc)
            logger.info("Selector wait timeout on page %d, continuing with fallback.", page_num)

        _scroll_to_load(page, click_show_more=False)  # Don't click buttons that may navigate away
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
                except Exception as exc:
                    logger.debug("Failed to get page title/url: %s", exc)
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
    2. Integrity check: skip update if scrape is incomplete (< 70% of peak)
    3. Removal confirmation: products must be missing 3 consecutive times to be marked removed
    4. Peak tracking: remember historical max product count for integrity validation
    
    Returns: (current_products, new_products, removed_products)
    Raises: ScrapeIncomplete if scrape returned too few products (should retry later)
    """
    MISS_THRESHOLD = 3  # Consecutive misses required to confirm removal
    INTEGRITY_RATIO = 0.7  # Min ratio of current/peak to accept update
    MIN_PRODUCTS_THRESHOLD = 5  # Below this count, always consider incomplete
    
    now = datetime.datetime.now(datetime.timezone.utc)
    
    try:
        # Load task and all existing products
        from models import MonitorTask
        task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
        if not task:
            logger.error("Task %s not found in database.", task_id)
            return [], [], []
        
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
            logger.info(
                "Task %s baseline initialized with %d products; skip email on first run.",
                task_id, len(current_products),
            )
            return current_products, [], []
        
        # EMPTY SCRAPE: Completely failed - raise exception for retry
        if not current_products:
            logger.warning("[ATOMIC] Task %s finished with 0 products - network failure suspected.", task_id)
            raise ScrapeIncomplete("Scrape returned 0 products")
        
        # INTEGRITY CHECK: Reject incomplete scrapes
        peak = task.peak_product_count or 0
        active_in_db = sum(1 for p in all_db_products if p.removed_at is None)
        reference_count = max(peak, active_in_db)
        
        if reference_count > 0:
            scrape_ratio = len(current_products) / reference_count
            if scrape_ratio < INTEGRITY_RATIO:
                logger.warning(
                    "[ATOMIC] Task %s: Scrape incomplete - got %d products, expected ~%d (ratio=%.2f < %.2f). "
                    "REJECTING this scrape - will retry later.",
                    task_id, len(current_products), reference_count, scrape_ratio, INTEGRITY_RATIO
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
        
        # Build lookup sets
        db_asins = {p.asin for p in all_db_products if p.asin}
        db_links = {p.product_link for p in all_db_products}
        current_asins = {item["asin"] for item in current_products if item.get("asin")}
        current_links = {item["link"] for item in current_products}
        
        # Filter noise from current scrape
        current_products = [item for item in current_products if not _is_noise_title(item.get("name"))]
        
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
        
        # === DETECT REMOVED PRODUCTS (with confirmation) ===
        removed_products: List[Dict[str, str]] = []
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
                    logger.info("Task %s: Product %s found again, resetting miss_count from %d to 0.",
                                task_id, p.asin or p.product_link[:50], p.miss_count)
                p.miss_count = 0
            else:
                # Product not found - increment miss count
                p.miss_count = (p.miss_count or 0) + 1
                logger.warning(
                    "[ATOMIC] Task %s: Product %s not found (miss_count=%d/%d)",
                    task_id, p.asin or p.product_link[:50], p.miss_count, MISS_THRESHOLD
                )
                
                if p.miss_count >= MISS_THRESHOLD:
                    # Confirmed removal after multiple consecutive misses
                    p.removed_at = now
                    removed_products.append({"name": p.name, "link": p.product_link})
                    logger.warning(
                        "[ATOMIC] Task %s: Product %s CONFIRMED REMOVED after %d consecutive misses.",
                        task_id, p.asin or p.product_link[:50], MISS_THRESHOLD
                    )
        
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

# Thread-safe activity tracking for timeout mechanism.
_last_activity_time: float = 0.0
_last_activity_lock = threading.Lock()
_partial_results: List[Dict[str, str]] = []  # Shared list for partial results.
_partial_results_lock = threading.Lock()


def _update_activity():
    """Update the last activity timestamp when progress is made."""
    global _last_activity_time
    with _last_activity_lock:
        _last_activity_time = time.time()


def _add_partial_result(product: Dict[str, str]):
    """Add a product to partial results (thread-safe)."""
    with _partial_results_lock:
        _partial_results.append(product)


def _get_partial_results() -> List[Dict[str, str]]:
    """Get a copy of partial results."""
    with _partial_results_lock:
        return list(_partial_results)


def _clear_partial_results():
    """Clear partial results."""
    global _partial_results
    with _partial_results_lock:
        _partial_results = []


def _get_seconds_since_activity() -> float:
    """Get seconds elapsed since last activity."""
    with _last_activity_lock:
        if _last_activity_time == 0:
            return 0.0
        return time.time() - _last_activity_time


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
    """Run the browser scrape in a child thread with activity-based timeout.
    
    The timeout is based on time since last activity (product captured or page visited),
    not total elapsed time. This allows long scrapes for stores with many products.
    """
    global _last_activity_time
    result: List[Dict[str, str]] = []
    error: List[Exception] = []

    # Reset activity tracker and partial results.
    with _last_activity_lock:
        _last_activity_time = time.time()
    _clear_partial_results()

    def _target():
        try:
            result.extend(_run_browser_scrape(task))
        except Exception as exc:
            error.append(exc)

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    
    # Poll for completion with activity-based timeout.
    start_time = time.time()
    while t.is_alive():
        t.join(timeout=5.0)  # Check every 5 seconds.
        if not t.is_alive():
            break
        
        elapsed = time.time() - start_time
        idle_time = _get_seconds_since_activity()
        
        # Timeout if no activity for _ACTIVITY_TIMEOUT seconds.
        if idle_time > _ACTIVITY_TIMEOUT:
            partial = _get_partial_results()
            logger.error(
                "Task %s scrape timed out: no activity for %.0fs (total elapsed: %.0fs). Returning %d partial products.",
                task.id, idle_time, elapsed, len(partial)
            )
            _kill_zombie_browsers()
            return partial  # Return partial results if any.
        
        # Hard limit as fallback (e.g., 10x the activity timeout).
        if elapsed > _SCRAPE_TIMEOUT:
            partial = _get_partial_results()
            logger.warning(
                "Task %s reached hard timeout of %ds with %d products captured.",
                task.id, _SCRAPE_TIMEOUT, len(partial)
            )
            _kill_zombie_browsers()
            return partial  # Return partial results.

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
    logger.warning("[DEBUG] Pre-warming homepage: %s", base_url)
    try:
        page.goto(base_url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(random.uniform(0.8, 1.6))
        logger.warning("[DEBUG] Homepage pre-warm completed.")
    except Exception as exc:
        logger.warning("[DEBUG] Pre-warm navigation failed: %s", exc)

    logger.warning("[DEBUG] Navigating to target URL: %s", task.url)
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
        "[data-action='a]",
    ]
    
    clicked_count = 0
    max_hovers = 15
    
    logger.warning("[DEBUG] Expanding dropdown menus...")
    
    for selector in dropdown_selectors:
        if clicked_count >= max_hovers:
            break
        try:
            locator = scope.locator(selector)
            count = locator.count()
            if count == 0:
                continue
            logger.warning("[DEBUG] Found %d elements matching '%s'", count, selector)
            for idx in range(min(count, max_hovers - clicked_count)):
                try:
                    target = locator.nth(idx)
                    # Only hover to reveal dropdown content - DO NOT click (would navigate away)
                    target.hover(timeout=2000)
                    time.sleep(0.5)  # Wait for dropdown to appear
                    _update_activity()
                    clicked_count += 1
                    logger.warning("[DEBUG] Hovered dropdown %d", clicked_count)
                except Exception as exc:
                    logger.debug("Dropdown hover failed: %s", exc)
                    continue
        except Exception as exc:
            logger.debug("Dropdown selector '%s' failed: %s", selector, exc)
            continue
    
    logger.warning("[DEBUG] Expanded %d dropdown menus via hover.", clicked_count)


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
                logger.warning("[DEBUG] Found nav scope with selector: %s", selector)
                return nav.first
        except Exception as exc:
            logger.debug("Failed to check nav element '%s': %s", selector, exc)
            continue
    return None


def _discover_storefront_tabs(page: Page, base_url: str) -> List[str]:
    """Find all sub-page links on an Amazon Storefront navigation bar."""
    seen: Set[str] = set()
    tab_urls: List[str] = []
    
    logger.warning("[DEBUG] Looking for nav scope...")
    nav_scope = _storefront_nav_scope(page)
    logger.warning("[DEBUG] Nav scope found: %s", nav_scope is not None)
    
    logger.warning("[DEBUG] Expanding storefront menus...")
    _expand_storefront_menus(page, nav_scope)
    logger.warning("[DEBUG] Menu expansion done.")

    for selector in _STOREFRONT_NAV_SELECTORS:
        scope = nav_scope or page
        logger.warning("[DEBUG] Trying selector: %s", selector)
        try:
            # Use count() first to avoid slow .all() on large sets
            locator = scope.locator(selector)
            count = locator.count()
            logger.warning("[DEBUG] Selector '%s' matched %d elements", selector, count)
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
            logger.warning("[DEBUG] Selector '%s' failed: %s", selector, exc)
            continue

    logger.warning("[DEBUG] Discovered %d storefront tab URLs.", len(tab_urls))
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
    logger.warning("[DEBUG] Page has %d unique ASINs (%d new, %d already seen from other pages).", 
                   len(page_asins), len(new_asins), len(duplicate_asins))

    for a in anchors:
        try:
            href = (a.get_attribute("href") or "").strip()
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
                logger.warning("[DEBUG] Skipping noise title: '%s' (ASIN=%s)", title, asin)
                continue

            products.append({"name": title, "link": link, "asin": asin})
            _add_partial_result({"name": title, "link": link, "asin": asin})  # Save to partial results.
            _update_activity()  # Mark progress when product captured.
            logger.warning("[DEBUG] Storefront product captured: %s (ASIN=%s) total=%d.", title, asin, len(products))
        except Exception as exc:
            logger.debug("Storefront anchor parse failed: %s", exc)

    return products


def _run_storefront_scrape(task: MonitorTask) -> List[Dict[str, str]]:
    """Scrape an Amazon Storefront by visiting each navigation tab."""
    seen_asins: Set[str] = set()
    logger.warning("[DEBUG] Starting storefront scrape for task=%s url=%s", task.id, task.url)
    for attempt in range(1, _MAX_CONTEXT_ATTEMPTS + 1):
        _check_cancel()
        logger.warning("[DEBUG] Storefront context attempt %d/%d", attempt, _MAX_CONTEXT_ATTEMPTS)
        try:
            pw, browser, page, base_url = _create_browser_context(task)
            logger.warning("[DEBUG] Browser context created successfully for task=%s", task.id)
        except RuntimeError as exc:
            logger.error("%s", exc)
            if attempt < _MAX_CONTEXT_ATTEMPTS:
                time.sleep(2.0 * attempt)
                continue
            return []
        try:
            # Wait for page to properly load before scrolling
            logger.warning("[DEBUG] Waiting for page content to load...")
            try:
                # Wait for body to have content
                page.wait_for_function("document.body.scrollHeight > 100", timeout=15000)
            except Exception as e:
                logger.warning("[DEBUG] Page height check failed: %s. Current URL: %s", e, page.url)
                # Try to wait for any visible content
                try:
                    page.wait_for_selector("body *", timeout=10000)
                except Exception:
                    pass
            
            # Log current page state for debugging
            current_height = page.evaluate("document.body.scrollHeight")
            current_url = page.url
            logger.warning("[DEBUG] Page state before scroll: height=%d, url=%s", current_height, current_url)
            
            # Collect products from the landing page first.
            logger.warning("[DEBUG] Scrolling to load landing page content...")
            _scroll_to_load(page, click_show_more=False)  # Don't click buttons that may navigate away
            products = _collect_asin_links_from_page(page, base_url, seen_asins)
            logger.warning("[DEBUG] Storefront landing: collected %d products.", len(products))

            # Discover and visit sub-pages.
            logger.warning("[DEBUG] Discovering storefront tabs...")
            tab_urls = _discover_storefront_tabs(page, base_url)
            logger.warning("[DEBUG] Discovered %d tab URLs.", len(tab_urls))
            current_url = page.url
            logger.warning("[DEBUG] Will visit %d storefront tabs (current: %s)", len(tab_urls), current_url)
            for idx, tab_url in enumerate(tab_urls, 1):
                _check_cancel()
                # Skip if it's the same page we already scraped.
                if urlparse(tab_url).path == urlparse(current_url).path:
                    logger.debug("Skipping same page: %s", tab_url)
                    continue
                logger.warning("[DEBUG] Visiting storefront tab %d/%d: %s", idx, len(tab_urls), tab_url)
                time.sleep(random.uniform(1.5, 3.0))
                _update_activity()  # Mark activity when visiting each tab (even if 0 new products)
                if not _navigate_with_retry(page, tab_url):
                    logger.warning("Storefront tab navigation failed: %s", tab_url)
                    continue
                if _is_blocked(page):
                    logger.error("Blocked on storefront tab: %s", tab_url)
                    break
                _scroll_to_load(page, click_show_more=False)  # Don't click buttons that may navigate away
                _update_activity()  # Mark activity after scroll completes
                tab_products = _collect_asin_links_from_page(page, base_url, seen_asins)
                products.extend(tab_products)
                logger.warning("[DEBUG] Tab %d/%d collected %d products, total=%d.",
                            idx, len(tab_urls), len(tab_products), len(products))

            logger.warning("[DEBUG] Storefront scrape complete: %d total products.", len(products))
            return products
        finally:
            browser.close()
            pw.stop()
    return []
