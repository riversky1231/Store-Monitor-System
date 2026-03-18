import io
import logging
import logging.handlers
import os
import sys
import threading
import subprocess
import tempfile
import time
from urllib.parse import urlparse
import pystray
from PIL import Image
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager

from database import run_migrations, SessionLocal
from models import SystemConfig
from scheduler import init_scheduler, scheduler
from utils import get_resource_path

# ---------------------------------------------------------------------------
# Persistent file logging — survives crashes, aids debugging.
# ---------------------------------------------------------------------------
_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0] if not getattr(sys, "frozen", False) else sys.executable)), "logs")
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_FILE = os.path.join(_LOG_DIR, "monitor.log")

_file_handler = logging.handlers.RotatingFileHandler(
    _LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
))
_fallback_stream = sys.stdout if sys.stdout is not None else (getattr(sys, "__stdout__", None) or io.StringIO())
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(_fallback_stream), _file_handler])
logger = logging.getLogger(__name__)

# Apply pending schema migrations before app startup.
run_migrations()


def _format_proxy_value(proxy: str) -> str:
    if not proxy:
        return ""
    parsed = urlparse(proxy)
    if parsed.scheme and parsed.hostname:
        safe = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            safe += f":{parsed.port}"
        return safe
    return proxy


def _log_proxy_configuration() -> None:
    http_proxy = os.environ.get("HTTP_PROXY", "").strip()
    https_proxy = os.environ.get("HTTPS_PROXY", "").strip()
    if not http_proxy and not https_proxy:
        logger.info("Proxy: not configured.")
        return
    if http_proxy and https_proxy and http_proxy == https_proxy:
        logger.info("Proxy: %s", _format_proxy_value(http_proxy))
        return
    if http_proxy:
        logger.info("Proxy (HTTP): %s", _format_proxy_value(http_proxy))
    if https_proxy:
        logger.info("Proxy (HTTPS): %s", _format_proxy_value(https_proxy))


def _apply_proxy_from_db() -> None:
    """Load proxy from SystemConfig and apply to process env for Playwright."""
    try:
        db = SessionLocal()
        config = db.query(SystemConfig).first()
        proxy = (config.proxy_url or "").strip() if config else ""
        if proxy:
            os.environ["HTTP_PROXY"] = proxy
            os.environ["HTTPS_PROXY"] = proxy
        else:
            os.environ.pop("HTTP_PROXY", None)
            os.environ.pop("HTTPS_PROXY", None)
    except Exception as exc:
        logger.warning("Failed to load proxy from database: %s", exc)
    finally:
        try:
            db.close()
        except Exception as close_exc:
            logger.debug("Error closing database session: %s", close_exc)


_apply_proxy_from_db()
_log_proxy_configuration()

# Browser installation lock to prevent race conditions
_browser_install_lock = threading.Lock()
_browser_installed = False

# Ensure Playwright browser is installed (runs in background, non-blocking).
def _ensure_playwright_browser():
    global _browser_installed
    try:
        with _browser_install_lock:
            if _browser_installed:
                return

            if _use_bundled_browsers():
                _browser_installed = True
                return

            browsers_dir = _get_playwright_browsers_dir()
            if _browsers_present(browsers_dir):
                _browser_installed = True
                return

            lock_path = _get_browser_install_lock_path(browsers_dir)
            lock_fd = _acquire_browser_install_lock(lock_path, stale_after_seconds=900)
            if lock_fd is None:
                logger.info("Playwright install already running; waiting for completion.")
                if _wait_for_browser_install(browsers_dir, lock_path, timeout_seconds=900):
                    _browser_installed = _browsers_present(browsers_dir)
                    return
                # Retry once if the lock cleared but browsers are still missing.
                lock_fd = _acquire_browser_install_lock(lock_path, stale_after_seconds=0)
                if lock_fd is None:
                    logger.warning("Playwright install lock is still held; skipping install.")
                    return

            try:
                _install_playwright_browser()
            finally:
                _release_browser_install_lock(lock_fd, lock_path)

            _browser_installed = _browsers_present(browsers_dir)
    except Exception as exc:
        logger.debug("Playwright browser ensure failed: %s", exc)


def _use_bundled_browsers() -> bool:
    if not getattr(sys, "frozen", False):
        return False
    bundled_browsers = os.path.join(os.path.dirname(sys.executable), "playwright-browsers")
    if os.path.isdir(bundled_browsers) and any(os.scandir(bundled_browsers)):
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", bundled_browsers)
        return True
    return False


def _get_playwright_browsers_dir() -> str:
    env_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if env_path:
        return env_path
    local_appdata = os.environ.get("LOCALAPPDATA", "")
    if local_appdata:
        return os.path.join(local_appdata, "ms-playwright")
    return os.path.join(tempfile.gettempdir(), "ms-playwright")


def _browsers_present(browsers_dir: str) -> bool:
    return bool(browsers_dir) and os.path.isdir(browsers_dir) and any(os.scandir(browsers_dir))


def _get_browser_install_lock_path(browsers_dir: str) -> str:
    lock_dir = browsers_dir or tempfile.gettempdir()
    os.makedirs(lock_dir, exist_ok=True)
    return os.path.join(lock_dir, ".playwright_install.lock")


def _acquire_browser_install_lock(lock_path: str, stale_after_seconds: int) -> int | None:
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.write(fd, f"{os.getpid()} {int(time.time())}".encode("utf-8"))
        return fd
    except FileExistsError:
        if stale_after_seconds > 0:
            try:
                age = time.time() - os.path.getmtime(lock_path)
            except OSError:
                age = 0
            if age > stale_after_seconds:
                try:
                    os.remove(lock_path)
                except OSError:
                    return None
                return _acquire_browser_install_lock(lock_path, stale_after_seconds=0)
        return None


def _release_browser_install_lock(lock_fd: int | None, lock_path: str) -> None:
    if lock_fd is None:
        return
    try:
        os.close(lock_fd)
    except OSError:
        pass
    try:
        os.remove(lock_path)
    except OSError:
        pass


def _wait_for_browser_install(browsers_dir: str, lock_path: str, timeout_seconds: int) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if _browsers_present(browsers_dir):
            return True
        if not os.path.exists(lock_path):
            return _browsers_present(browsers_dir)
        time.sleep(2.0)
    return _browsers_present(browsers_dir)


def _install_playwright_browser() -> None:
    if getattr(sys, "frozen", False):
        # In PyInstaller frozen mode, sys.executable is the EXE itself, not Python.
        # Use playwright's bundled Node.js driver directly to install the browser.
        from playwright._impl._driver import get_driver_executable
        driver_exe = str(get_driver_executable())
        result = subprocess.run(
            [driver_exe, "install", "chromium"],
            capture_output=True, timeout=600,
        )
    else:
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"],
            capture_output=True, timeout=600,
        )
    if result.returncode != 0:
        stderr = (result.stderr or b"").decode("utf-8", errors="ignore").strip()
        stdout = (result.stdout or b"").decode("utf-8", errors="ignore").strip()
        logger.warning("Playwright install failed (code=%s): %s", result.returncode, stderr or stdout)

threading.Thread(target=_ensure_playwright_browser, daemon=True).start()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_scheduler()
    yield

app = FastAPI(title="Store Monitor Web App", lifespan=lifespan)

@app.middleware("http")
async def setup_guard(request: Request, call_next):
    """Redirect to /setup when first-run configuration is not yet complete."""
    path = request.url.path
    if path.startswith("/setup") or path.startswith("/static"):
        return await call_next(request)
    # If env-var password is configured, skip DB setup check (dev/server mode).
    if os.getenv("MONITOR_WEB_PASSWORD", "").strip():
        return await call_next(request)
    db = SessionLocal()
    try:
        config = db.query(SystemConfig).first()
        if not config or not config.setup_complete:
            return RedirectResponse(url="/setup", status_code=302)
    finally:
        db.close()
    return await call_next(request)

# Static and Templates paths
static_dir = get_resource_path("static")
templates_dir = get_resource_path("templates")

# Ensure static subdirs exist in CWD for logs/db if needed, 
# but for serving static, we use the bundled one.
app.mount("/static", StaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory=templates_dir)

# Add custom filter to convert UTC to Beijing time (UTC+8)
import datetime
def to_beijing_time(dt):
    """Convert UTC datetime to Beijing time (UTC+8)."""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        # Assume naive datetime is UTC
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    beijing_tz = datetime.timezone(datetime.timedelta(hours=8))
    beijing_dt = dt.astimezone(beijing_tz)
    return beijing_dt.strftime('%m-%d %H:%M')

templates.env.filters["beijing"] = to_beijing_time

from routes.web import router as web_router, public_router
app.include_router(public_router)
app.include_router(web_router)

import webbrowser
import tkinter as tk
from tkinter import scrolledtext
import queue

# Create a queue for thread-safe cross-thread UI updates
log_queue = queue.Queue()

class ConsoleEmulator:
    def __init__(self, root):
        self.root = root
        self.root.title("Amazon Store Monitor - Console")
        self.root.geometry("800x500")
        self.root.configure(bg='black')
        
        # Don't destroy on close, just hide
        self.root.protocol("WM_DELETE_WINDOW", self.hide)
        
        self.text_area = scrolledtext.ScrolledText(
            self.root, bg='black', fg='#00FF00', # Classic matrix green
            insertbackground='white', font=('Consolas', 10),
            padx=10, pady=10
        )
        self.text_area.pack(expand=True, fill='both')
        self.hide() # Start hidden
        
        # Start the queue consumer
        self.root.after(100, self.process_queue)

    def write(self, text):
        log_queue.put(text)

    def flush(self):
        pass

    def isatty(self):
        return False

    def fileno(self):
        raise io.UnsupportedOperation("fileno")

    def process_queue(self):
        while True:
            try:
                msg = log_queue.get_nowait()
                self.text_area.insert(tk.END, msg)
                self.text_area.see(tk.END)
            except queue.Empty:
                break
        self.root.after(100, self.process_queue)

    def show(self):
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()

    def hide(self):
        self.root.withdraw()

def open_browser():
    url = "http://127.0.0.1:8000"
    print(f"正在打开监控后台: {url}")
    # open_new=0 asks the browser to reuse an existing window rather than a new tab.
    webbrowser.open(url, new=0)

def quit_app(icon, item, root):
    print("正在彻底退出程序...")
    icon.stop()
    if scheduler.running:
        scheduler.shutdown(wait=False)
    root.quit()
    os._exit(0)

def start_tray(console_ui, root):
    icon_path = get_resource_path(os.path.join("static", "icon.png"))
    try:
        image = Image.open(icon_path)
    except Exception:
        image = Image.new('RGB', (64, 64), color=(255, 165, 0))

    menu = pystray.Menu(
        pystray.MenuItem("打开监控后台", open_browser, default=True),
        pystray.MenuItem("查看运行日志 (Show Console)", lambda: console_ui.show()),
        pystray.MenuItem("退出程序", lambda icon, item: quit_app(icon, item, root))
    )

    icon = pystray.Icon("AmazonStoreMonitor", image, "亚马逊上新监控", menu)
    icon.run()

def run_server_sync():
    # Use config to run without signal handlers for thread safety
    config = uvicorn.Config(app, host="127.0.0.1", port=8000, log_level="info", loop="asyncio")
    server = uvicorn.Server(config)
    server.run()

if __name__ == "__main__":
    # Prevent multiple instances on Windows using a named mutex.
    import ctypes
    _mutex = ctypes.windll.kernel32.CreateMutexW(None, False, "AmazonStoreMonitor_SingleInstance")
    if ctypes.windll.kernel32.GetLastError() == 183:  # ERROR_ALREADY_EXISTS
        # Another instance is already running — exit silently without opening a new browser tab.
        sys.exit(0)

    # Initialize Tkinter
    root = tk.Tk()
    root.withdraw() # Main root is hidden, we use ConsoleEmulator window
    
    console_ui = ConsoleEmulator(tk.Toplevel(root))
    
    # Redirect stdout and stderr
    sys.stdout = console_ui
    sys.stderr = console_ui
    # Route logging stream handler to the console UI (avoid None stream in GUI mode).
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setStream(console_ui)

    print("=========================================")
    print("   🚀 Amazon Store Monitor 启动完成")
    print("=========================================")
    print("程序已转入后台运行，请查看系统托盘图标。")
    print("双击托盘图标或右键菜单可打开此日志窗口。")
    
    # Start Backend Server
    threading.Thread(target=run_server_sync, daemon=True).start()
    
    # Start Tray Icon
    threading.Thread(target=start_tray, args=(console_ui, root), daemon=True).start()
    
    # Initial browser open
    threading.Timer(2.0, open_browser).start()
    
    # Tkinter main loop
    root.mainloop()
