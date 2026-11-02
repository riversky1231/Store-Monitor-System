"""Dashboard routes."""
import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import case
from sqlalchemy.orm import Session

from database import get_db
from models import MonitorTask, ProductItem
from security import require_admin_auth
from traffic_metrics import get_snapshot as get_traffic_snapshot

from . import templates

logger = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(require_admin_auth)])


def _empty_traffic_snapshot() -> dict:
    return {
        "started_at": "",
        "started_since_label": "0s",
        "totals": {
            "request_count": 0,
            "upload_bytes": 0,
            "upload_human": "0.0000 GB",
            "download_bytes": 0,
            "download_human": "0.0000 GB",
            "completed_task_count": 0,
        },
        "active_task": None,
        "last_completed_task": None,
    }


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, db: Session = Depends(get_db)):
    tasks_count = db.query(MonitorTask).count()
    active_tasks_count = db.query(MonitorTask).filter(MonitorTask.is_active.is_(True)).count()
    products_count = db.query(ProductItem).filter(ProductItem.removed_at.is_(None)).count()
    recent_rows = (
        db.query(
            ProductItem.id,
            ProductItem.name,
            ProductItem.product_link,
            ProductItem.discovered_at,
            MonitorTask.name.label("task_name"),
        )
        .outerjoin(MonitorTask, MonitorTask.id == ProductItem.task_id)
        .filter(ProductItem.removed_at.is_(None))
        .order_by(
            case((ProductItem.discovered_at.is_(None), 1), else_=0),
            ProductItem.discovered_at.desc(),
            ProductItem.id.desc(),
        )
        .limit(10)
        .all()
    )
    recent_products = [
        {
            "name": row.name or "",
            "task_name": row.task_name or "已删除任务",
            "discovered_at": row.discovered_at,
            "product_link": row.product_link or "",
        }
        for row in recent_rows
    ]
    try:
        traffic_stats = get_traffic_snapshot()
    except Exception:
        logger.exception("Failed to render traffic snapshot on dashboard.")
        traffic_stats = _empty_traffic_snapshot()

    return templates.TemplateResponse(request, "dashboard.html", {
        "request": request,
        "tasks_count": tasks_count,
        "active_tasks_count": active_tasks_count,
        "products_count": products_count,
        "recent_products": recent_products,
        "traffic_stats": traffic_stats,
    })
