"""API routes for frontend polling."""
import asyncio

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from database import get_db
from models import MonitorTask
from scheduler import get_inflight_task_ids, get_network_retry_status, get_queue_snapshot
from security import require_admin_auth
from utils import probe_http_text, response_looks_blocked

router = APIRouter(dependencies=[Depends(require_admin_auth)])


@router.get("/api/queue-status")
async def queue_status(db: Session = Depends(get_db)):
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
        items.append({"id": running_id, "name": task.name if task else f"Task-{running_id}", "status": "running"})

    for tid in waiting_ids:
        task = tasks_by_id.get(tid)
        items.append({"id": tid, "name": task.name if task else f"Task-{tid}", "status": "waiting"})

    return JSONResponse({"items": items, "total": len(items)})


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
    network_healthy = pending_count == 0
    cooldown_seconds = status.get("cooldown_seconds", 0)

    if network_healthy:
        summary = "网络正常，所有任务已恢复抓取。"
        detail = ""
    else:
        summary = f"网络异常中，{pending_count} 个任务等待重试。"
        detail = (
            f"当前网络检测仍未恢复，已有 {pending_count} 个任务等待重试。"
            " 建议优先切换网络、重连代理/VPN，恢复后系统会自动重试。"
        )

    return JSONResponse(
        {
            "active": True,
            "state": "issue",
            "pending_count": pending_count,
            "pending_preview": preview_names,
            "network_healthy": network_healthy,
            "event_id": status.get("alert_event_id", 0),
            "last_issue_at": status.get("last_issue_at"),
            "last_check": status.get("last_check"),
            "cooldown_seconds": cooldown_seconds,
            "title": "抓取网络异常提醒",
            "message": summary,
            "detail": detail,
        }
    )


@router.get("/api/network-check")
async def network_check():
    """Check if Amazon is accessible and measure response time."""
    def _do_check():
        results = {
            "success": False,
            "message": "",
            "response_time_ms": None,
            "details": []
        }

        test_urls = [
            ("https://www.amazon.com", "Amazon 主站"),
            ("https://www.amazon.com/robots.txt", "Amazon robots.txt"),
        ]

        all_ok = True
        total_time = 0
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        for url, name in test_urls:
            probe = probe_http_text(url, headers=headers, timeout=15, max_bytes=2000)
            if probe.error_kind == "timeout":
                results["details"].append({
                    "name": name,
                    "status": "❌ 超时",
                    "time_ms": None,
                    "note": "连接超时 (>15s)"
                })
                all_ok = False
                continue
            if probe.error_kind:
                results["details"].append({
                    "name": name,
                    "status": "❌ 失败",
                    "time_ms": None,
                    "note": probe.error_message[:50]
                })
                all_ok = False
                continue

            elapsed = probe.elapsed_ms or 0
            total_time += elapsed
            status = probe.status_code or 0
            if status == 200:
                if response_looks_blocked(probe.final_url or url, probe.body_text):
                    results["details"].append({
                        "name": name,
                        "status": "⚠️ 可能被限制",
                        "time_ms": round(elapsed),
                        "note": "检测到验证码/机器人检查"
                    })
                    all_ok = False
                else:
                    results["details"].append({
                        "name": name,
                        "status": "✅ 正常",
                        "time_ms": round(elapsed),
                        "note": ""
                    })
            elif status == 503:
                results["details"].append({
                    "name": name,
                    "status": "⚠️ 服务不可用",
                    "time_ms": round(elapsed),
                    "note": f"HTTP {status} - 可能被临时限制"
                })
                all_ok = False
            else:
                results["details"].append({
                    "name": name,
                    "status": "✅ 可访问",
                    "time_ms": round(elapsed),
                    "note": f"HTTP {status}"
                })

        if total_time > 0:
            results["response_time_ms"] = round(total_time / len(test_urls))

        if all_ok:
            avg_time = results["response_time_ms"] or 0
            if avg_time < 500:
                results["success"] = True
                results["message"] = f"✅ 网络状态良好！平均响应 {avg_time}ms，适合抓取。"
            elif avg_time < 2000:
                results["success"] = True
                results["message"] = f"⚠️ 网络较慢，平均响应 {avg_time}ms，可以抓取但可能较慢。"
            else:
                results["success"] = False
                results["message"] = f"⚠️ 网络很慢，平均响应 {avg_time}ms，建议稍后再试。"
        else:
            results["success"] = False
            results["message"] = "❌ 网络连接异常，请检查代理设置或稍后再试。"

        return results

    results = await asyncio.to_thread(_do_check)
    return JSONResponse(results)
