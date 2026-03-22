"""Task management routes."""
from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from database import get_db
from models import Category, MonitorTask, PendingImport
from scheduler import (
    EMPTY_ALERT_THRESHOLD,
    get_inflight_task_ids,
    queue_monitor_task,
    remove_scheduled_task,
    schedule_task,
)
from security import (
    normalize_recipients,
    require_admin_auth,
    validate_monitor_target_url,
)

from . import templates
from ._shared import _build_url, _group_error_redirect, _task_redirect

router = APIRouter(dependencies=[Depends(require_admin_auth)])


# ---------------------------------------------------------------------------
# Group detail  (/tasks/group/<id>)
# ---------------------------------------------------------------------------

@router.get("/tasks/group/{group_id}", response_class=HTMLResponse)
async def group_detail(
    request: Request,
    group_id: int,
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    search: str = Query(""),
):
    group = db.query(Category).filter(Category.id == group_id).first()
    if not group:
        return RedirectResponse(url="/tasks", status_code=302)

    q = db.query(MonitorTask).filter(MonitorTask.category_id == group_id)
    if search:
        q = q.filter(MonitorTask.name.ilike(f"%{search}%"))
    total = q.count()
    total_pages = max(1, (total + 10 - 1) // 10)
    page = min(page, total_pages)
    tasks = q.offset((page - 1) * 10).limit(10).all()

    all_groups = db.query(Category).order_by(Category.name).all()
    pending_imports = db.query(PendingImport).order_by(PendingImport.id).all()

    return templates.TemplateResponse("tasks.html", {
        "request": request,
        "group": group,
        "tasks": tasks,
        "empty_alert_threshold": EMPTY_ALERT_THRESHOLD,
        "inflight_ids": get_inflight_task_ids(),
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "search": search,
        "all_groups": all_groups,
        "pending_imports": pending_imports,
    })


# ---------------------------------------------------------------------------
# Task CRUD  (all redirect back to the owning group)
# ---------------------------------------------------------------------------

@router.post("/tasks/group/{group_id}/add")
async def add_task(
    group_id: int,
    name: str = Form(...),
    url: str = Form(...),
    selector: str = Form(""),
    task_type: str = Form("search"),
    check_interval_hours: int = Form(...),
    recipients: str = Form(...),
    is_active: bool = Form(False),
    db: Session = Depends(get_db),
):
    task_name = name.strip()
    resolved_type = task_type.strip() if task_type.strip() in ("search", "storefront") else "search"
    css_selector = selector.strip() or (
        "storefront-auto" if resolved_type == "storefront"
        else "div[data-component-type='s-search-result']"
    )
    if not task_name:
        return _group_error_redirect(group_id, "任务名称不能为空。")
    if resolved_type == "search" and not css_selector:
        return _group_error_redirect(group_id, "CSS 选择器不能为空。")
    if check_interval_hours < 1 or check_interval_hours > 168:
        return _group_error_redirect(group_id, "检查频率必须在 1-168 小时之间。")
    if db.query(MonitorTask).filter(MonitorTask.name == task_name).first():
        return _group_error_redirect(group_id, f"店铺名称「{task_name}」已存在，请使用不同的名称。")

    try:
        target_url = validate_monitor_target_url(url)
        recipients_value = normalize_recipients(recipients)
    except ValueError as exc:
        return _group_error_redirect(group_id, str(exc))

    new_task = MonitorTask(
        name=task_name,
        url=target_url,
        task_type=resolved_type,
        selector=css_selector,
        check_interval_hours=check_interval_hours,
        recipients=recipients_value,
        category_id=group_id,
        is_active=is_active,
    )
    db.add(new_task)
    db.commit()
    db.refresh(new_task)
    schedule_task(new_task)

    total_tasks = db.query(MonitorTask).filter(MonitorTask.category_id == group_id).count()
    last_page = (total_tasks + 10 - 1) // 10
    return RedirectResponse(
        url=f"/tasks/group/{group_id}?page={last_page}&highlight={new_task.id}",
        status_code=303,
    )


@router.post("/tasks/{task_id}/edit")
async def edit_task(
    task_id: int,
    name: str = Form(...),
    url: str = Form(...),
    selector: str = Form(""),
    task_type: str = Form("search"),
    check_interval_hours: int = Form(...),
    recipients: str = Form(...),
    is_active: bool = Form(False),
    db: Session = Depends(get_db),
):
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if not task:
        return RedirectResponse(url="/tasks", status_code=303)
    gid = task.category_id

    task_name = name.strip()
    resolved_type = task_type.strip() if task_type.strip() in ("search", "storefront") else "search"
    css_selector = selector.strip() or (
        "storefront-auto" if resolved_type == "storefront"
        else "div[data-component-type='s-search-result']"
    )
    if not task_name:
        return _group_error_redirect(gid, "任务名称不能为空。")
    if resolved_type == "search" and not css_selector:
        return _group_error_redirect(gid, "CSS 选择器不能为空。")
    dup = db.query(MonitorTask).filter(MonitorTask.name == task_name, MonitorTask.id != task_id).first()
    if dup:
        return _group_error_redirect(gid, f"店铺名称「{task_name}」已存在，请使用不同的名称。")
    if check_interval_hours < 1 or check_interval_hours > 168:
        return _group_error_redirect(gid, "检查频率必须在 1-168 小时之间。")

    try:
        target_url = validate_monitor_target_url(url)
        recipients_value = normalize_recipients(recipients)
    except ValueError as exc:
        return _group_error_redirect(gid, str(exc))

    config_changed = task.url != target_url or task.selector != css_selector or task.task_type != resolved_type
    task.name = task_name
    task.url = target_url
    task.task_type = resolved_type
    task.selector = css_selector
    task.check_interval_hours = check_interval_hours
    task.recipients = recipients_value
    task.is_active = is_active
    if config_changed:
        task.consecutive_empty_count = 0
        task.health_state = "healthy"
    db.commit()
    db.refresh(task)

    remove_scheduled_task(task.id)
    schedule_task(task)
    return _task_redirect(task)


@router.post("/tasks/{task_id}/toggle")
async def toggle_task(task_id: int, db: Session = Depends(get_db)):
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if task:
        task.is_active = not task.is_active
        db.commit()
        db.refresh(task)
        if task.is_active:
            schedule_task(task)
        else:
            remove_scheduled_task(task.id)
        return _task_redirect(task)
    return RedirectResponse(url="/tasks", status_code=303)


@router.post("/tasks/{task_id}/delete")
async def delete_task(task_id: int, db: Session = Depends(get_db)):
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if task:
        gid = task.category_id
        remove_scheduled_task(task.id)
        db.delete(task)
        db.commit()
        base = f"/tasks/group/{gid}" if gid else "/tasks"
        return RedirectResponse(url=base, status_code=303)
    return RedirectResponse(url="/tasks", status_code=303)


@router.post("/tasks/batch-move")
async def batch_move_tasks(
    request: Request,
    db: Session = Depends(get_db),
):
    form = await request.form()
    task_ids_raw = form.getlist("task_ids")
    target_group_id = int(form.get("target_group_id", 0))
    source_group_id = int(form.get("source_group_id", 0))

    target_group = db.query(Category).filter(Category.id == target_group_id).first()
    if not target_group:
        return _group_error_redirect(source_group_id, "目标分组不存在。")

    moved = 0
    for raw_id in task_ids_raw:
        try:
            tid = int(raw_id)
        except (ValueError, TypeError):
            continue
        task = db.query(MonitorTask).filter(MonitorTask.id == tid).first()
        if task and task.category_id != target_group_id:
            task.category_id = target_group_id
            moved += 1

    db.commit()
    return RedirectResponse(
        url=_build_url(
            f"/tasks/group/{source_group_id}",
            batch_moved=moved,
            target_name=target_group.name,
        ),
        status_code=303,
    )


@router.post("/tasks/batch-delete")
async def batch_delete_tasks(
    request: Request,
    db: Session = Depends(get_db),
):
    form = await request.form()
    task_ids_raw = form.getlist("task_ids")
    source_group_id = int(form.get("source_group_id", 0))

    deleted = 0
    for raw_id in task_ids_raw:
        try:
            tid = int(raw_id)
        except (ValueError, TypeError):
            continue
        task = db.query(MonitorTask).filter(MonitorTask.id == tid).first()
        if task:
            remove_scheduled_task(task.id)
            db.delete(task)
            deleted += 1

    db.commit()
    return RedirectResponse(
        url=_build_url(f"/tasks/group/{source_group_id}", batch_deleted=deleted),
        status_code=303,
    )


@router.post("/tasks/batch-update-interval")
async def batch_update_interval(
    request: Request,
    db: Session = Depends(get_db),
):
    form = await request.form()
    task_ids_raw = form.getlist("task_ids")
    source_group_id = int(form.get("source_group_id", 0))
    new_interval = int(form.get("new_interval", 24))

    if new_interval < 1 or new_interval > 168:
        return _group_error_redirect(source_group_id, "检查频率必须在 1-168 小时之间。")

    updated = 0
    for raw_id in task_ids_raw:
        try:
            tid = int(raw_id)
        except (ValueError, TypeError):
            continue
        task = db.query(MonitorTask).filter(MonitorTask.id == tid).first()
        if task:
            task.check_interval_hours = new_interval
            remove_scheduled_task(task.id)
            schedule_task(task)
            updated += 1

    db.commit()
    return RedirectResponse(
        url=_build_url(
            f"/tasks/group/{source_group_id}",
            batch_updated=updated,
            new_interval=new_interval,
        ),
        status_code=303,
    )


@router.post("/tasks/group/{group_id}/claim-pending")
async def claim_pending_imports(
    group_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    group = db.query(Category).filter(Category.id == group_id).first()
    if not group:
        return RedirectResponse(
            url=_build_url("/tasks", error="分组不存在。"),
            status_code=303,
        )

    form = await request.form()
    pending_ids_raw = form.getlist("pending_ids")

    existing_urls = {r[0] for r in db.query(MonitorTask.url).all()}
    existing_names = {r[0] for r in db.query(MonitorTask.name).all()}
    claimed = 0

    for raw_id in pending_ids_raw:
        try:
            pid = int(raw_id)
        except (ValueError, TypeError):
            continue
        pending = db.query(PendingImport).filter(PendingImport.id == pid).first()
        if not pending:
            continue
        if pending.url in existing_urls or pending.name in existing_names:
            db.delete(pending)
            continue

        task = MonitorTask(
            name=pending.name,
            url=pending.url,
            selector=pending.selector,
            check_interval_hours=pending.check_interval_hours,
            recipients=pending.recipients,
            category_id=group_id,
            is_active=pending.is_active,
        )
        db.add(task)
        existing_urls.add(pending.url)
        existing_names.add(pending.name)
        db.delete(pending)
        claimed += 1

    db.commit()

    new_tasks = (
        db.query(MonitorTask)
        .filter(MonitorTask.category_id == group_id)
        .order_by(MonitorTask.id.desc())
        .limit(claimed)
        .all()
    )
    for t in new_tasks:
        schedule_task(t)

    return RedirectResponse(
        url=_build_url(f"/tasks/group/{group_id}", claimed=claimed),
        status_code=303,
    )


@router.post("/tasks/clear-pending")
async def clear_pending_imports(db: Session = Depends(get_db)):
    db.query(PendingImport).delete()
    db.commit()
    return RedirectResponse(url=_build_url("/tasks", pending_cleared=1), status_code=303)


@router.post("/tasks/{task_id}/run")
async def run_task_now(task_id: int, db: Session = Depends(get_db)):
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found.")

    queued = queue_monitor_task(task_id)
    extra = "run_started=1" if queued else "already_running=1"
    return _task_redirect(task, extra)


@router.post("/tasks/{task_id}/reset-health")
async def reset_task_health(task_id: int, db: Session = Depends(get_db)):
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if task:
        task.consecutive_empty_count = 0
        task.health_state = "healthy"
        db.commit()
    return RedirectResponse(url="/", status_code=303)
