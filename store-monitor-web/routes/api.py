"""API routes for frontend polling."""

import asyncio
import logging

from fastapi import APIRouter, Depends, Path, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from database import get_db
from models import MonitorTask
from scheduler import (
    get_network_retry_status,
    get_queue_snapshot,
    get_report_delivery_status,
    retry_report_delivery_by_id,
    retry_report_delivery_now,
    run_manual_network_check,
)
from security import require_admin_auth
from traffic_metrics import get_snapshot as get_traffic_snapshot

router = APIRouter(dependencies=[Depends(require_admin_auth)])
logger = logging.getLogger(__name__)


@router.get("/api/queue-status")
async def queue_status(
    report_page: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    running_id, waiting_ids = get_queue_snapshot()

    # 收集所有需要查询的 task id
    all_ids = [tid for tid in ([running_id] if running_id else []) + list(waiting_ids)]

    # 批量查询，避免 N+1 问题
    tasks_by_id: dict[int, MonitorTask] = {}
    if all_ids:
        rows = db.query(MonitorTask).filter(MonitorTask.id.in_(all_ids)).all()
        tasks_by_id = {t.id: t for t in rows}

    items: list[dict] = []
    if running_id:
        task = tasks_by_id.get(running_id)
        items.append(
            {
                "id": running_id,
                "name": task.name if task else f"Task-{running_id}",
                "status": "running",
            }
        )

    for tid in waiting_ids:
        task = tasks_by_id.get(tid)
        items.append(
            {
                "id": tid,
                "name": task.name if task else f"Task-{tid}",
                "status": "waiting",
            }
        )

    return JSONResponse(
        {
            "items": items,
            "total": len(items),
            "report_delivery": get_report_delivery_status(db, page=report_page),
        }
    )


@router.post("/api/report-delivery/retry")
async def retry_report_delivery(
    report_page: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    """Retry only deliverable outbox rows; held review reports remain blocked."""
    result = await asyncio.to_thread(retry_report_delivery_now)
    db.expire_all()
    return JSONResponse(
        {
            "attempted": int(result.get("attempted", 0)),
            "sent": int(result.get("sent", 0)),
            "failed": int(result.get("failed", 0)),
            "busy": bool(result.get("busy", 0)),
            "report_delivery": get_report_delivery_status(db, page=report_page),
        }
    )


@router.post("/api/report-delivery/{report_id}/retry")
async def retry_single_report_delivery(
    report_id: int = Path(..., ge=1),
    report_page: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    """Resend one selected report when its retained content is available."""
    logger.info("Report resend request received: report_id=%d", report_id)
    result = await asyncio.to_thread(retry_report_delivery_by_id, report_id)
    logger.info(
        "Report resend request completed: report_id=%d attempted=%d sent=%d failed=%d busy=%s unavailable=%s",
        report_id,
        int(result.get("attempted", 0)),
        int(result.get("sent", 0)),
        int(result.get("failed", 0)),
        bool(result.get("busy", 0)),
        bool(result.get("unavailable", 0)),
    )
    db.expire_all()
    return JSONResponse(
        {
            "attempted": int(result.get("attempted", 0)),
            "sent": int(result.get("sent", 0)),
            "failed": int(result.get("failed", 0)),
            "busy": bool(result.get("busy", 0)),
            "unavailable": bool(result.get("unavailable", 0)),
            "report_delivery": get_report_delivery_status(db, page=report_page),
        }
    )


@router.get("/api/network-alert-status")
async def network_alert_status(db: Session = Depends(get_db)):
    status = get_network_retry_status()
    pending_task_ids = status.get("pending_tasks", [])[:3]

    # 批量查询，避免 N+1 问题
    preview_names: list[str] = []
    if pending_task_ids:
        rows = db.query(MonitorTask).filter(MonitorTask.id.in_(pending_task_ids)).all()
        tasks_by_id = {t.id: t for t in rows}
        preview_names = [
            tasks_by_id[tid].name if tid in tasks_by_id else f"Task-{tid}"
            for tid in pending_task_ids
        ]

    pending_count = status.get("pending_count", 0)
    network_healthy = bool(status.get("network_healthy", True))
    cooldown_seconds = status.get(
        "popup_cooldown_seconds", status.get("cooldown_seconds", 0)
    )
    issue_active = bool(status.get("alert_active"))
    recovery_event_id = int(status.get("recovery_event_id", 0) or 0)
    alert_event_id = int(status.get("alert_event_id", 0) or 0)

    if issue_active:
        state = "issue"
        summary = f"网络异常中，{pending_count} 个任务待人工处理。"
        detail = (
            f"当前网络检测仍未恢复，已有 {pending_count} 个任务待人工处理。"
            " 建议优先切换网络、重连代理/VPN，恢复后再手动重试。"
        )
        title = "抓取网络异常提醒"
        event_id = alert_event_id
    elif recovery_event_id and recovery_event_id >= alert_event_id:
        state = "recovered"
        summary = (
            status.get("last_recovery_message")
            or "网络访问已恢复，可继续人工处理之前受影响的任务。"
        )
        detail = "之前受影响的任务不会自动重试，请按需手动处理。"
        title = "抓取网络恢复提醒"
        event_id = recovery_event_id
    else:
        state = "idle"
        summary = "网络正常，当前没有待人工处理任务。"
        detail = ""
        title = "抓取网络状态"
        event_id = 0

    return JSONResponse(
        {
            "active": state != "idle",
            "state": state,
            "pending_count": pending_count,
            "pending_preview": preview_names,
            "network_healthy": network_healthy,
            "event_id": event_id,
            "last_issue_at": status.get("last_issue_at"),
            "last_check": status.get("last_check"),
            "cooldown_seconds": cooldown_seconds,
            "title": title,
            "message": summary,
            "detail": detail,
        }
    )


@router.get("/api/network-check")
async def network_check():
    """Check whether Amazon is accessible without triggering any automatic retries."""
    results = await asyncio.to_thread(run_manual_network_check)
    return JSONResponse(results)


@router.get("/api/traffic-stats")
async def traffic_stats():
    return JSONResponse(get_traffic_snapshot())
