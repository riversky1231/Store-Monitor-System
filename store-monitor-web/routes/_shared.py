"""Shared helpers for route modules."""
from fastapi.responses import RedirectResponse
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# Re-export from routes/__init__.py for convenience
from . import templates


def _build_url(path: str, **params) -> str:
    """Build a URL with a properly encoded query string."""
    split = urlsplit(path)
    query = {
        key: value
        for key, value in parse_qsl(split.query, keep_blank_values=True)
    }
    query.update(
        {
            key: value
            for key, value in params.items()
            if value is not None and value != ""
        }
    )
    return urlunsplit(("", "", split.path, urlencode(query), split.fragment))


def _safe_local_target(target: str | None, fallback: str) -> str:
    candidate = (target or "").strip()
    if candidate.startswith("/") and not candidate.startswith("//"):
        return candidate
    return fallback


def _redirect_back(target: str | None, fallback: str, **params) -> RedirectResponse:
    return RedirectResponse(
        url=_build_url(_safe_local_target(target, fallback), **params),
        status_code=303,
    )


def _group_error_redirect(group_id: int | None, message: str) -> RedirectResponse:
    """Redirect to group or task list page with error message."""
    path = f"/tasks/group/{group_id}" if group_id else "/tasks"
    return RedirectResponse(url=_build_url(path, error=message), status_code=303)


def _task_redirect(task, next_url: str | None = None, **params) -> RedirectResponse:
    """Redirect to task's group page."""
    gid = task.category_id
    base = f"/tasks/group/{gid}" if gid else "/tasks"
    return _redirect_back(next_url, base, **params)
