"""Group management routes."""
from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import desc, func as sa_func
from sqlalchemy.orm import Session
from urllib.parse import quote

from database import get_db
from models import Category, MonitorTask, PendingImport
from scheduler import force_stop_queue, remove_scheduled_task, schedule_task
from security import (
    normalize_recipients,
    require_admin_auth,
    validate_monitor_target_url,
)

from . import templates

router = APIRouter(dependencies=[Depends(require_admin_auth)])


@router.get("/tasks", response_class=HTMLResponse)
async def groups_page(
    request: Request,
    db: Session = Depends(get_db),
    search: str = Query(""),
):
    q = db.query(Category)
    if search:
        q = q.filter(Category.name.ilike(f"%{search}%"))
    groups = q.order_by(Category.created_at.desc()).all()

    # Pre-compute task counts per group.
    counts_q = (
        db.query(MonitorTask.category_id, sa_func.count(MonitorTask.id))
        .filter(MonitorTask.category_id.isnot(None))
        .group_by(MonitorTask.category_id)
        .all()
    )
    task_counts = dict(counts_q)
    pending_count = db.query(PendingImport).count()

    return templates.TemplateResponse("groups.html", {
        "request": request,
        "groups": groups,
        "task_counts": task_counts,
        "search": search,
        "pending_count": pending_count,
    })


@router.post("/tasks/group-create")
async def create_group(
    name: str = Form(...),
    db: Session = Depends(get_db),
):
    group_name = name.strip()
    if not group_name:
        return RedirectResponse(url="/tasks?error=" + quote("分组名称不能为空。"), status_code=303)
    existing = db.query(Category).filter(Category.name == group_name).first()
    if existing:
        return RedirectResponse(url="/tasks?error=" + quote("分组名称已存在。"), status_code=303)
    group = Category(name=group_name)
    db.add(group)
    db.commit()
    return RedirectResponse(url="/tasks", status_code=303)


@router.post("/tasks/group/{group_id}/rename")
async def rename_group(
    group_id: int,
    name: str = Form(...),
    db: Session = Depends(get_db),
):
    new_name = name.strip()
    if not new_name:
        return RedirectResponse(url="/tasks?error=" + quote("分组名称不能为空。"), status_code=303)
    group = db.query(Category).filter(Category.id == group_id).first()
    if not group:
        return RedirectResponse(url="/tasks?error=" + quote("分组不存在。"), status_code=303)
    existing = db.query(Category).filter(Category.name == new_name, Category.id != group_id).first()
    if existing:
        return RedirectResponse(url="/tasks?error=" + quote("分组名称已存在。"), status_code=303)
    group.name = new_name
    db.commit()
    return RedirectResponse(url="/tasks", status_code=303)


@router.get("/tasks/search-store", response_class=HTMLResponse)
async def search_store_global(
    request: Request,
    q: str = Query(""),
    db: Session = Depends(get_db),
):
    """Search for a store (task) by name and redirect to its group with highlight."""
    store_name = q.strip()
    if not store_name:
        return RedirectResponse(url="/tasks", status_code=302)
    task = db.query(MonitorTask).filter(MonitorTask.name == store_name).first()
    if not task:
        # Fallback: try partial match
        task = db.query(MonitorTask).filter(MonitorTask.name.ilike(f"%{store_name}%")).first()
    if not task or not task.category_id:
        return RedirectResponse(
            url="/tasks?error=" + quote(f"未找到名为「{store_name}」的店铺。"), status_code=303,
        )
    # Calculate which page the task is on within its group.
    tasks_before = (
        db.query(MonitorTask)
        .filter(MonitorTask.category_id == task.category_id, MonitorTask.id < task.id)
        .count()
    )
    from . import TASKS_PAGE_SIZE
    page = tasks_before // TASKS_PAGE_SIZE + 1
    return RedirectResponse(
        url=f"/tasks/group/{task.category_id}?page={page}&highlight={task.id}",
        status_code=302,
    )


@router.post("/tasks/group/{group_id}/delete")
async def delete_group(group_id: int, db: Session = Depends(get_db)):
    group = db.query(Category).filter(Category.id == group_id).first()
    if group:
        # Remove scheduled jobs for tasks in this group, then cascade-delete.
        for task in group.tasks:
            remove_scheduled_task(task.id)
        db.delete(group)
        db.commit()
    return RedirectResponse(url="/tasks", status_code=303)


@router.post("/tasks/run-all")
async def run_all_tasks(db: Session = Depends(get_db)):
    """Queue every active task for execution. Consolidated email is sent after all finish."""
    tasks = db.query(MonitorTask).filter(MonitorTask.is_active.is_(True)).all()
    queued = 0
    from scheduler import queue_monitor_task
    for task in tasks:
        if queue_monitor_task(task.id):
            queued += 1
    return RedirectResponse(url=f"/tasks?run_all=1&queued={queued}", status_code=303)


@router.post("/tasks/queue/stop")
async def stop_queue():
    running_id, cleared = force_stop_queue()
    parts = [f"queue_stopped=1", f"cleared={cleared}"]
    if running_id is not None:
        parts.append(f"running={running_id}")
    return RedirectResponse(url="/tasks?" + "&".join(parts), status_code=303)
