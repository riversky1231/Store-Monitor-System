"""Task management routes."""
import logging

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy import func as sa_func, or_
from sqlalchemy.orm import Session

from database import get_db
from models import Category, MonitorTask, PendingImport
from scraper import (
    accept_pending_integrity_review,
    clear_pending_integrity_review,
    get_pending_integrity_review,
)
from scheduler import (
    EMPTY_ALERT_THRESHOLD,
    build_group_digest_scope,
    build_task_digest_scope,
    get_inflight_task_ids,
    queue_monitor_batch,
    remove_scheduled_task,
    resolve_manual_review_report,
    schedule_task,
)
from security import (
    normalize_recipients,
    require_admin_auth,
    validate_monitor_target_url,
)

from . import templates
from ._shared import _build_url, _group_error_redirect, _redirect_back, _task_redirect

router = APIRouter(dependencies=[Depends(require_admin_auth)])
logger = logging.getLogger(__name__)


class TaskOrderPayload(BaseModel):
    task_ids: list[int] = Field(min_length=0, max_length=5000)


def _task_ordering():
    return (MonitorTask.sort_order.asc(), MonitorTask.id.asc())


def _next_task_sort_order(db: Session, group_id: int | None) -> int:
    maximum = (
        db.query(sa_func.max(MonitorTask.sort_order))
        .filter(MonitorTask.category_id == group_id)
        .scalar()
    )
    return int(maximum) + 1 if maximum is not None else 0


def _clear_manual_review_fields(task: MonitorTask) -> None:
    task.manual_review_pending = False
    task.manual_review_kind = None
    task.manual_review_scope_key = None
    task.manual_review_scope_label = None
    task.manual_review_scope_kind = None
    task.manual_review_started_at = None


def _saved_manual_review_scope(task: MonitorTask) -> dict | None:
    scope_key = (task.manual_review_scope_key or "").strip()
    if not scope_key:
        return None
    return {
        "key": scope_key,
        "label": (task.manual_review_scope_label or task.name or "待确认任务").strip(),
        "kind": (task.manual_review_scope_kind or "group").strip() or "group",
    }


def _current_path_query(request: Request) -> str:
    query = request.url.query
    return f"{request.url.path}?{query}" if query else request.url.path


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
    tasks = q.order_by(*_task_ordering()).offset((page - 1) * 10).limit(10).all()
    for task in tasks:
        task.pending_integrity_review = get_pending_integrity_review(task.id)

    all_groups = db.query(Category).order_by(Category.name).all()
    pending_imports = db.query(PendingImport).order_by(PendingImport.id).all()
    ordered_group_tasks = (
        db.query(MonitorTask.id, MonitorTask.name)
        .filter(MonitorTask.category_id == group_id)
        .order_by(*_task_ordering())
        .all()
    )

    return templates.TemplateResponse(request, "tasks.html", {
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
        "ordered_group_tasks": ordered_group_tasks,
        "current_path_query": _current_path_query(request),
    })


@router.post("/tasks/group/{group_id}/run")
async def run_group_now(group_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    group = db.query(Category).filter(Category.id == group_id).first()
    if not group:
        return _redirect_back(next_url, "/tasks")

    tasks = (
        db.query(MonitorTask)
        .filter(
            MonitorTask.category_id == group_id,
            MonitorTask.is_active.is_(True),
            or_(
                MonitorTask.manual_review_pending.is_(False),
                MonitorTask.manual_review_pending.is_(None),
            ),
        )
        .order_by(*_task_ordering())
        .all()
    )
    digest_scope = build_group_digest_scope(group.id, group.name, unique_run=True)
    queued = queue_monitor_batch([task.id for task in tasks], digest_scope)

    return _redirect_back(next_url, f"/tasks/group/{group_id}", run_group=1, queued=queued)


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
    next_url: str = Form(""),
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
        sort_order=_next_task_sort_order(db, group_id),
        is_active=is_active,
    )
    db.add(new_task)
    db.commit()
    db.refresh(new_task)
    schedule_task(new_task)

    if next_url:
        return _redirect_back(
            next_url,
            f"/tasks/group/{group_id}",
            created=1,
            highlight=new_task.id,
        )

    total_tasks = db.query(MonitorTask).filter(MonitorTask.category_id == group_id).count()
    last_page = (total_tasks + 10 - 1) // 10
    return RedirectResponse(
        url=f"/tasks/group/{group_id}?page={last_page}&highlight={new_task.id}",
        status_code=303,
    )


@router.post("/tasks/group/{group_id}/reorder", response_class=JSONResponse)
async def reorder_group_tasks(
    group_id: int,
    payload: TaskOrderPayload,
    db: Session = Depends(get_db),
):
    group_exists = db.query(Category.id).filter(Category.id == group_id).first()
    if not group_exists:
        raise HTTPException(status_code=404, detail="Group not found.")

    tasks = (
        db.query(MonitorTask)
        .filter(MonitorTask.category_id == group_id)
        .order_by(MonitorTask.id.asc())
        .all()
    )
    current_ids = {task.id for task in tasks}
    submitted_ids = payload.task_ids
    submitted_set = set(submitted_ids)
    if (
        len(submitted_ids) != len(submitted_set)
        or len(submitted_ids) != len(current_ids)
        or submitted_set != current_ids
    ):
        logger.warning(
            "Rejected task reorder: group_id=%d submitted_count=%d current_count=%d duplicates=%s",
            group_id,
            len(submitted_ids),
            len(current_ids),
            len(submitted_ids) != len(submitted_set),
        )
        raise HTTPException(
            status_code=400,
            detail="Task order must contain every current group task exactly once.",
        )

    positions = {task_id: index for index, task_id in enumerate(submitted_ids)}
    for task in tasks:
        task.sort_order = positions[task.id]
    db.commit()
    logger.info("Task order updated: group_id=%d task_count=%d", group_id, len(tasks))
    return JSONResponse({"ok": True, "count": len(tasks)})


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
    next_url: str = Form(""),
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
    review_scope_key = task.manual_review_scope_key if config_changed and task.manual_review_pending else None
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
        task.last_failure_reason = None
        _clear_manual_review_fields(task)
        clear_pending_integrity_review(task.id)
    db.commit()
    db.refresh(task)
    if review_scope_key:
        resolve_manual_review_report(
            review_scope_key,
            task.id,
            resolution_message="人工处理：任务配置已更新",
        )

    remove_scheduled_task(task.id)
    schedule_task(task)
    return _task_redirect(task, next_url)


@router.post("/tasks/{task_id}/toggle")
async def toggle_task(task_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if task:
        review_scope_key = task.manual_review_scope_key if task.manual_review_pending else None
        task.is_active = not task.is_active
        if not task.is_active:
            task.consecutive_empty_count = 0
            task.consecutive_failure_count = 0
            task.health_state = "healthy"
            task.last_failure_at = None
            task.last_failure_reason = None
            _clear_manual_review_fields(task)
            clear_pending_integrity_review(task.id)
        db.commit()
        db.refresh(task)
        if task.is_active:
            schedule_task(task)
        else:
            remove_scheduled_task(task.id)
            resolve_manual_review_report(
                review_scope_key,
                task.id,
                resolution_message="人工处理：任务已停用",
            )
        return _task_redirect(task, next_url)
    return RedirectResponse(url="/tasks", status_code=303)


@router.post("/tasks/{task_id}/delete")
async def delete_task(task_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if task:
        gid = task.category_id
        review_scope_key = task.manual_review_scope_key if task.manual_review_pending else None
        remove_scheduled_task(task.id)
        clear_pending_integrity_review(task.id)
        db.delete(task)
        db.commit()
        resolve_manual_review_report(
            review_scope_key,
            task_id,
            resolution_message="人工处理：任务已删除",
        )
        base = f"/tasks/group/{gid}" if gid else "/tasks"
        return _redirect_back(next_url, base)
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
    return_to = (form.get("return_to") or "").strip()

    target_group = db.query(Category).filter(Category.id == target_group_id).first()
    if not target_group:
        return _group_error_redirect(source_group_id, "目标分组不存在。")

    moved = 0
    next_position = _next_task_sort_order(db, target_group_id)
    for raw_id in task_ids_raw:
        try:
            tid = int(raw_id)
        except (ValueError, TypeError):
            continue
        task = db.query(MonitorTask).filter(MonitorTask.id == tid).first()
        if task and task.category_id != target_group_id:
            task.category_id = target_group_id
            task.sort_order = next_position
            next_position += 1
            moved += 1

    db.commit()
    return _redirect_back(
        return_to,
        f"/tasks/group/{source_group_id}",
        batch_moved=moved,
        target_name=target_group.name,
    )


@router.post("/tasks/batch-delete")
async def batch_delete_tasks(
    request: Request,
    db: Session = Depends(get_db),
):
    form = await request.form()
    task_ids_raw = form.getlist("task_ids")
    source_group_id = int(form.get("source_group_id", 0))
    return_to = (form.get("return_to") or "").strip()

    deleted = 0
    resolved_reviews: list[tuple[str | None, int]] = []
    for raw_id in task_ids_raw:
        try:
            tid = int(raw_id)
        except (ValueError, TypeError):
            continue
        task = db.query(MonitorTask).filter(MonitorTask.id == tid).first()
        if task:
            if task.manual_review_pending:
                resolved_reviews.append((task.manual_review_scope_key, task.id))
            remove_scheduled_task(task.id)
            clear_pending_integrity_review(task.id)
            db.delete(task)
            deleted += 1

    db.commit()
    for scope_key, task_id in resolved_reviews:
        resolve_manual_review_report(
            scope_key,
            task_id,
            resolution_message="人工处理：任务已删除",
        )
    return _redirect_back(return_to, f"/tasks/group/{source_group_id}", batch_deleted=deleted)


@router.post("/tasks/batch-update-interval")
async def batch_update_interval(
    request: Request,
    db: Session = Depends(get_db),
):
    form = await request.form()
    task_ids_raw = form.getlist("task_ids")
    source_group_id = int(form.get("source_group_id", 0))
    new_interval = int(form.get("new_interval", 24))
    return_to = (form.get("return_to") or "").strip()

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
    return _redirect_back(
        return_to,
        f"/tasks/group/{source_group_id}",
        batch_updated=updated,
        new_interval=new_interval,
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
    return_to = (form.get("return_to") or "").strip()

    existing_urls = {r[0] for r in db.query(MonitorTask.url).all()}
    existing_names = {r[0] for r in db.query(MonitorTask.name).all()}
    claimed = 0
    next_position = _next_task_sort_order(db, group_id)

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
            sort_order=next_position,
            is_active=pending.is_active,
        )
        db.add(task)
        next_position += 1
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

    return _redirect_back(return_to, f"/tasks/group/{group_id}", claimed=claimed)


@router.post("/tasks/clear-pending")
async def clear_pending_imports(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    db.query(PendingImport).delete()
    db.commit()
    return _redirect_back(next_url, "/tasks", pending_cleared=1)


@router.post("/tasks/{task_id}/run")
async def run_task_now(task_id: int, request: Request, db: Session = Depends(get_db)):
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found.")

    if task.manual_review_pending:
        digest_scope = _saved_manual_review_scope(task) or build_task_digest_scope(task.id, task.name)
        clear_pending_integrity_review(task.id)
    else:
        digest_scope = build_task_digest_scope(task.id, task.name)
    queued = queue_monitor_batch([task_id], digest_scope) == 1
    param_name = "run_started" if queued else "already_running"
    form = await request.form()
    redirect_to = (
        (request.query_params.get("next") or "").strip()
        or (form.get("next") or "").strip()
    )
    return _task_redirect(task, redirect_to, **{param_name: 1})


@router.post("/tasks/{task_id}/reset-health")
async def reset_task_health(task_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if task:
        review_scope_key = task.manual_review_scope_key if task.manual_review_pending else None
        task.consecutive_empty_count = 0
        task.consecutive_failure_count = 0
        task.health_state = "healthy"
        _clear_manual_review_fields(task)
        task.last_failure_at = None
        task.last_failure_reason = None
        clear_pending_integrity_review(task.id)
        db.commit()
        resolve_manual_review_report(
            review_scope_key,
            task.id,
            resolution_message="人工处理：健康状态已重置",
        )
        remove_scheduled_task(task.id)
        if task.is_active:
            schedule_task(task)
    return _task_redirect(task, next_url) if task else RedirectResponse(url="/", status_code=303)


@router.post("/tasks/{task_id}/accept-manual-review")
async def accept_task_manual_review(task_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if not task:
        return RedirectResponse(url="/tasks", status_code=303)

    base_url = f"/tasks/group/{task.category_id}" if task.category_id else "/tasks"
    review_scope_key = task.manual_review_scope_key
    if task.manual_review_kind == "integrity":
        result = accept_pending_integrity_review(db, task_id)
        if not result:
            return _redirect_back(next_url, base_url, error="当前没有待确认的低数量抓取结果。")

        remove_scheduled_task(task.id)
        if task.is_active:
            schedule_task(task)
        resolve_manual_review_report(
            review_scope_key,
            task.id,
            resolution_message="人工确认：已接受本次低数量抓取结果",
        )
        return _redirect_back(
            next_url,
            base_url,
            normalized=1,
            normalized_count=result["current_count"],
            normalized_removed=result["removed_count"],
        )

    task.consecutive_empty_count = 0
    task.consecutive_failure_count = 0
    task.health_state = "healthy"
    _clear_manual_review_fields(task)
    task.last_failure_at = None
    task.last_failure_reason = None
    clear_pending_integrity_review(task.id)
    db.commit()
    remove_scheduled_task(task.id)
    if task.is_active:
        schedule_task(task)
    resolve_manual_review_report(
        review_scope_key,
        task.id,
        resolution_message="人工确认：已设为正常",
    )
    return _redirect_back(next_url, base_url, review_cleared=1)


@router.post("/tasks/{task_id}/ignore-manual-review")
async def ignore_task_manual_review(task_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if not task:
        return RedirectResponse(url="/tasks", status_code=303)

    base_url = f"/tasks/group/{task.category_id}" if task.category_id else "/tasks"
    review_scope_key = task.manual_review_scope_key
    task.consecutive_empty_count = 0
    task.consecutive_failure_count = 0
    task.health_state = "healthy"
    _clear_manual_review_fields(task)
    task.last_failure_at = None
    task.last_failure_reason = None
    clear_pending_integrity_review(task.id)
    db.commit()

    remove_scheduled_task(task.id)
    if task.is_active:
        schedule_task(task)
    resolve_manual_review_report(
        review_scope_key,
        task.id,
        resolution_message="人工处理：已忽略本次结果，保留上次运行记忆",
    )
    return _redirect_back(next_url, base_url, review_ignored=1)


@router.post("/tasks/{task_id}/retry-manual-review")
async def retry_task_manual_review(task_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if not task:
        return RedirectResponse(url="/tasks", status_code=303)

    base_url = f"/tasks/group/{task.category_id}" if task.category_id else "/tasks"
    digest_scope = _saved_manual_review_scope(task) or build_task_digest_scope(task.id, task.name)
    clear_pending_integrity_review(task.id)

    queued = queue_monitor_batch([task_id], digest_scope) == 1
    param_name = "run_started" if queued else "already_running"
    return _redirect_back(next_url, base_url, **{param_name: 1})
