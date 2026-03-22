"""Routes package - shared helpers for route modules."""
from fastapi.templating import Jinja2Templates

from utils import get_resource_path, to_beijing_time

# Shared constants
TASKS_PAGE_SIZE = 10
IMPORT_MAX_BYTES = 50 * 1024 * 1024  # 50 MB
SQLITE_MAGIC = b"SQLite format 3\x00"

# Initialize templates (shared across all route modules)
templates = Jinja2Templates(directory=get_resource_path("templates"))
templates.env.filters["beijing"] = to_beijing_time

from ._shared import _group_error_redirect as group_error_redirect
from ._shared import _task_redirect as task_redirect


def register_routes(app):
    """Wire all route modules into the FastAPI app."""
    from .api import router as api_router
    from .dashboard import router as dashboard_router
    from .groups import router as groups_router
    from .settings import public_router, router as settings_router
    from .tasks import router as tasks_router

    app.include_router(public_router)    # /setup
    app.include_router(dashboard_router) # /
    app.include_router(api_router)       # /api/*
    app.include_router(groups_router)    # /tasks, /tasks/group-create, etc.
    app.include_router(tasks_router)     # /tasks/group/{id}, /tasks/{id}, etc.
    app.include_router(settings_router)  # /settings


from .dashboard import router
from .settings import public_router
