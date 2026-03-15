"""Dashboard routes."""
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import desc
from sqlalchemy.orm import Session

from database import get_db
from models import MonitorTask, ProductItem
from scheduler import EMPTY_ALERT_THRESHOLD
from security import require_admin_auth

from . import templates

router = APIRouter(dependencies=[Depends(require_admin_auth)])


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, db: Session = Depends(get_db)):
    tasks_count = db.query(MonitorTask).count()
    active_tasks_count = db.query(MonitorTask).filter(MonitorTask.is_active.is_(True)).count()
    products_count = db.query(ProductItem).filter(ProductItem.removed_at.is_(None)).count()
    recent_products = (
        db.query(ProductItem)
        .filter(ProductItem.removed_at.is_(None))
        .order_by(ProductItem.discovered_at.desc())
        .limit(10)
        .all()
    )
    unhealthy_tasks = (
        db.query(MonitorTask)
        .filter(MonitorTask.is_active.is_(True))
        .filter(MonitorTask.consecutive_empty_count > 0)
        .order_by(MonitorTask.consecutive_empty_count.desc(), MonitorTask.id.asc())
        .all()
    )

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "tasks_count": tasks_count,
        "active_tasks_count": active_tasks_count,
        "products_count": products_count,
        "recent_products": recent_products,
        "unhealthy_tasks": unhealthy_tasks,
        "empty_alert_threshold": EMPTY_ALERT_THRESHOLD,
    })
