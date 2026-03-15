"""Routes package - shared helpers for route modules."""
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from urllib.parse import quote

from utils import get_resource_path

# Shared constants
TASKS_PAGE_SIZE = 10
IMPORT_MAX_BYTES = 50 * 1024 * 1024  # 50 MB
SQLITE_MAGIC = b"SQLite format 3\x00"

# Initialize templates (shared across all route modules)
templates = Jinja2Templates(directory=get_resource_path("templates"))

# Shared helper functions
def group_error_redirect(group_id: int, message: str) -> RedirectResponse:
    """Redirect to group page with error message."""
    return RedirectResponse(
        url=f"/tasks/group/{group_id}?error={quote(message)}",
        status_code=303,
    )


def task_redirect(task, extra: str = "") -> RedirectResponse:
    """Redirect to task's group page."""
    gid = task.category_id
    base = f"/tasks/group/{gid}" if gid else "/tasks"
    url = f"{base}?{extra}" if extra else base
    return RedirectResponse(url=url, status_code=303)


# NOTE:
# This module only provides shared constants, templates, and helper functions.
# The active app wiring lives in routes/web.py.
