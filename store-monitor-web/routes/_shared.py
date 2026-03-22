"""Shared helpers for route modules."""
from fastapi.responses import RedirectResponse
from urllib.parse import urlencode

# Re-export from routes/__init__.py for convenience
from . import templates


def _build_url(path: str, **params) -> str:
    """Build a URL with a properly encoded query string."""
    filtered = {
        key: value
        for key, value in params.items()
        if value is not None and value != ""
    }
    if not filtered:
        return path
    return f"{path}?{urlencode(filtered)}"


def _group_error_redirect(group_id: int | None, message: str) -> RedirectResponse:
    """Redirect to group or task list page with error message."""
    path = f"/tasks/group/{group_id}" if group_id else "/tasks"
    return RedirectResponse(url=_build_url(path, error=message), status_code=303)


def _task_redirect(task, extra: str = "") -> RedirectResponse:
    """Redirect to task's group page."""
    from models import MonitorTask
    gid = task.category_id
    base = f"/tasks/group/{gid}" if gid else "/tasks"
    url = f"{base}?{extra}" if extra else base
    return RedirectResponse(url=url, status_code=303)
