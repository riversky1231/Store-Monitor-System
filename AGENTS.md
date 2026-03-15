# Repository Guidelines

## Project Structure & Module Organization
- `store-monitor/` contains the legacy CLI monitor (`monitor.py`) and is not actively developed.
- `store-monitor-web/` is the active FastAPI desktop/web console app.
- Key modules live in `store-monitor-web/`. Entry points and UI live in `app.py`, `routes/`, `templates/`, and `static/`. Data and migrations live in `models.py`, `database.py`, and `alembic/`. Core behavior lives in `scraper.py`, `scheduler.py`, and `security.py`.
- Tests are under `store-monitor-web/tests/`.

## Build, Test, and Development Commands
Run all commands from `store-monitor-web/`.

```bash
pip install -r requirements.txt
python -m playwright install chromium
python app.py
```
- Installs dependencies, downloads Chromium for Playwright, and starts the dev server at `http://127.0.0.1:8000`.

```bash
pytest -q tests/
python -m unittest discover -s tests -v
```
- Runs pytest or unittest suites.

```bash
alembic upgrade head
alembic revision --autogenerate -m "description"
```
- Applies or generates database migrations.

```bash
build.bat
python -m PyInstaller --noconfirm --clean AmazonStoreMonitor.spec
```
- Builds the Windows EXE (batch script is the one-click path).

## Coding Style & Naming Conventions
- Python code uses standard 4-space indentation; follow existing module structure and naming.
- No formatter/linter is configured in the repo; keep changes consistent with current style.
- Use clear, descriptive function and variable names; avoid abbreviations unless already established.

## Testing Guidelines
- Tests live in `store-monitor-web/tests/` and follow `pytest.ini` (`test_*.py`).
- Prefer pytest for new tests; keep fixtures in `tests/conftest.py`.
- When touching scraping or scheduling logic, add or update tests in the relevant module test file (for example `test_scraper.py`).

## Commit & Pull Request Guidelines
- Git history is not available in this workspace, so no commit convention can be inferred.
- Use concise, imperative commit subjects (for example `Fix scheduler retry logic`).
- PRs should include a short summary, linked issue (if any), and screenshots for UI changes in `templates/` or `static/`.

## Security & Configuration Tips
- For local development, `MONITOR_WEB_DISABLE_AUTH=1` disables auth.
- SMTP secrets can be overridden with `STORE_MONITOR_SMTP_PASSWORD`.
- Non-localhost deployments require HTTPS by default; review `MONITOR_WEB_REQUIRE_HTTPS` before release.
