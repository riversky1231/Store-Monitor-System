# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Amazon Store Monitor - a Windows desktop application (system tray EXE + FastAPI web console) that monitors Amazon store pages for product changes and sends email notifications. The UI and commit messages are in Chinese; code is in English.

## Commands

```bash
# All commands run from store-monitor-web/
cd store-monitor-web

# Install dependencies
pip install -r requirements.txt
python -m playwright install chromium

# Run the dev server (opens http://127.0.0.1:8000)
python app.py

# Run tests
pytest -q tests/
python -m unittest discover -s tests -v

# Run a single test file
pytest tests/test_scraper.py -v

# Run a single test by name
pytest tests/test_scraper.py -v -k "test_function_name"

# Database migrations (Alembic)
alembic upgrade head
alembic revision --autogenerate -m "description"

# Build Windows EXE — option A: one-click (run on Windows)
build.bat                              # lets you choose onefile (portable) or onedir (faster startup)

# Build Windows EXE — option B: manual
set PLAYWRIGHT_BROWSERS_PATH=%cd%\\playwright-browsers
python -m playwright install chromium
python -c "from PIL import Image; Image.open('static/icon.png').save('static/icon.ico')"
python -m PyInstaller --noconfirm --clean AmazonStoreMonitor.spec
# Output: dist/AmazonStoreMonitor.exe
```

## Architecture

The project has two sub-projects:
- **`store-monitor/`** - Legacy CLI-only monitor (single `monitor.py`, uses requests + BeautifulSoup). Not actively developed.
- **`store-monitor-web/`** - Active project. FastAPI web app with system tray integration.

### store-monitor-web Module Map

- **`app.py`** - Entry point. Sets up FastAPI app, logging, lifespan (scheduler init), setup-guard middleware, system tray (pystray + tkinter ConsoleEmulator), and single-instance mutex. Runs uvicorn in a daemon thread.
- **`models.py`** - SQLAlchemy ORM models: `SystemConfig`, `Category`, `MonitorTask`, `PendingImport`, `ProductItem`.
- **`database.py`** - SQLAlchemy engine/session (`sqlite:///./monitor.db`). Runs Alembic migrations on startup; falls back to legacy `ALTER TABLE` migrations if Alembic is unavailable.
- **`scraper.py`** - Playwright-based browser scraping. Two modes: `_run_search_scrape` (paginated search results) and `_run_storefront_scrape` (Amazon Storefront tab navigation). Runs in a child thread with 600s timeout. `_sync_products_to_db` handles diff logic (new/removed/restored products, ASIN-based dedup).
- **`scheduler.py`** - APScheduler + serial execution queue. All task scrapes run through a single `_queue_worker` thread (one at a time) to avoid anti-bot detection. Handles retry logic, health state tracking (consecutive empty scrape alerts), email notifications (per-task + consolidated digest), and product retention cleanup.
- **`security.py`** - HTTP Basic Auth with rate limiting, Fernet encryption for SMTP passwords and admin password, URL validation (blocks private/loopback IPs), email format validation.
- **`routes/web.py`** - All web routes. Two routers: `public_router` (setup wizard) and `router` (auth-protected). Handles dashboard, group (Category) CRUD, task CRUD, batch import from `.db` files (new-format with categories and legacy format via PendingImport), batch move/delete, queue status API.
- **`utils.py`** - `get_resource_path()` for PyInstaller `_MEIPASS` compatibility.
- **`templates/`** - Jinja2 HTML templates: `layout.html` (base), `dashboard.html`, `groups.html`, `tasks.html`, `settings.html`, `setup.html`.

### Key Design Decisions

- **Serial execution queue**: All scrape tasks execute one at a time via `queue.Queue` + single worker thread to prevent parallel Amazon requests triggering anti-bot.
- **Dual migration strategy**: Alembic preferred; `_run_legacy_migrations()` uses `PRAGMA table_info` + `ALTER TABLE ADD COLUMN` as fallback for packaged EXE environments.
- **ASIN-based deduplication**: Products are matched by ASIN first, then by canonical link. Old records without ASIN get backfilled from subsequent scrapes.
- **Consolidated digest emails**: Changes from multiple tasks are batched and sent as a single email after the queue drains.
- **First-run setup flow**: `/setup` wizard initializes admin password + SMTP before any other route is accessible (enforced by `setup_guard` middleware).

### Data Model Relationships

```
Category (group) --1:N--> MonitorTask --1:N--> ProductItem
PendingImport (temporary staging for legacy .db imports)
SystemConfig (singleton: SMTP settings, admin password, retention config)
```

## Environment Variables

Key env vars for development (see README.md for full list):
- `MONITOR_WEB_DISABLE_AUTH=1` - Skip auth during local dev
- `MONITOR_WEB_PASSWORD` - Set admin password without DB setup
- `STORE_MONITOR_SMTP_PASSWORD` - Override DB-stored SMTP password
- `HTTPS_PROXY` / `HTTP_PROXY` - Proxy for Playwright browser
