import os
import sqlite3
import tempfile

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import desc, func as sa_func
from sqlalchemy.orm import Session
from urllib.parse import quote

from database import get_db
from models import Category, MonitorTask, PendingImport, SystemConfig, ProductItem
from scheduler import (
    EMPTY_ALERT_THRESHOLD,
    force_stop_queue,
    get_inflight_task_ids,
    get_queue_snapshot,
    queue_monitor_task,
    remove_scheduled_task,
    schedule_task,
)
from security import (
    encrypt_secret,
    normalize_recipients,
    require_admin_auth,
    validate_monitor_target_url,
)
from utils import get_resource_path

router = APIRouter(dependencies=[Depends(require_admin_auth)])
public_router = APIRouter()  # No auth — used for setup wizard.
templates = Jinja2Templates(directory=get_resource_path("templates"))

# Add custom filter to convert UTC to Beijing time (UTC+8)
import datetime as _dt
def _to_beijing_time(dt):
    """Convert UTC datetime to Beijing time (UTC+8)."""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    beijing_tz = _dt.timezone(_dt.timedelta(hours=8))
    beijing_dt = dt.astimezone(beijing_tz)
    return beijing_dt.strftime('%m-%d %H:%M')

templates.env.filters["beijing"] = _to_beijing_time

_TASKS_PAGE_SIZE = 10
_IMPORT_MAX_BYTES = 50 * 1024 * 1024  # 50 MB
_SQLITE_MAGIC = b"SQLite format 3\x00"


def _group_error_redirect(group_id: int | None, message: str) -> RedirectResponse:
    if group_id:
        return RedirectResponse(
            url=f"/tasks/group/{group_id}?error={quote(message)}", status_code=303,
        )
    return RedirectResponse(
        url=f"/tasks?error={quote(message)}", status_code=303,
    )


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

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
        .order_by(desc(MonitorTask.consecutive_empty_count), MonitorTask.id.asc())
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


# ---------------------------------------------------------------------------
# Groups overview   (/tasks)
# ---------------------------------------------------------------------------

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
    page = tasks_before // _TASKS_PAGE_SIZE + 1
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
    for task in tasks:
        if queue_monitor_task(task.id):
            queued += 1
    return RedirectResponse(url=f"/tasks?run_all=1&queued={queued}", status_code=303)


@router.post("/tasks/queue/stop")
async def stop_queue():
    """Force stop the running task and clear the queue."""
    running_id, cleared = force_stop_queue()
    parts = ["queue_stopped=1", f"cleared={cleared}"]
    if running_id is not None:
        parts.append(f"running={running_id}")
    return RedirectResponse(url="/tasks?" + "&".join(parts), status_code=303)


# ---------------------------------------------------------------------------
# Group import  (/tasks/group-import)
# ---------------------------------------------------------------------------

@router.post("/tasks/group-import")
async def import_tasks_to_group(
    request: Request,
    db: Session = Depends(get_db),
):
    form = await request.form()
    target_group_id_raw = form.get("group_id", "")
    db_file: UploadFile = form["db_file"]

    # Resolve optional target group (only required for legacy .db without categories).
    target_group_id = None
    if target_group_id_raw:
        try:
            target_group_id = int(target_group_id_raw)
        except (ValueError, TypeError):
            pass

    redirect_base = "/tasks"

    filename = db_file.filename or ""
    if not filename.lower().endswith(".db"):
        return RedirectResponse(url=f"{redirect_base}?error=" + quote("请上传 .db 格式的数据库文件。"), status_code=303)

    content = await db_file.read()
    if len(content) > _IMPORT_MAX_BYTES:
        return RedirectResponse(url=f"{redirect_base}?error=" + quote("文件过大，最大支持 50 MB。"), status_code=303)
    if len(content) < 100 or not content.startswith(_SQLITE_MAGIC):
        return RedirectResponse(url=f"{redirect_base}?error=" + quote("文件不是有效的 SQLite 数据库。"), status_code=303)

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        src = sqlite3.connect(tmp_path)
        src.row_factory = sqlite3.Row
        tables = {r[0] for r in src.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "monitor_tasks" not in tables:
            src.close()
            return RedirectResponse(url=f"{redirect_base}?error=" + quote("数据库中未找到 monitor_tasks 表。"), status_code=303)

        has_categories = "categories" in tables

        if has_categories:
            # New-format DB: auto-import groups + tasks.
            result = _import_with_categories(src, db)
            src.close()
            imported, skipped, groups_created = result
            db.commit()
            for task in imported:
                db.refresh(task)
                schedule_task(task)
            parts = [f"imported={len(imported)}", f"skipped={skipped}", f"groups={groups_created}"]
            return RedirectResponse(
                url=f"{redirect_base}?import_ok=1&{'&'.join(parts)}",
                status_code=303,
            )
        else:
            # Legacy DB: save to pending_imports for manual group assignment.
            pending_count, skipped = _import_legacy_to_pending(src, db)
            src.close()
            db.commit()
            return RedirectResponse(
                url=f"{redirect_base}?pending_ok=1&pending={pending_count}&skipped={skipped}",
                status_code=303,
            )

    except Exception as exc:
        return RedirectResponse(url=f"{redirect_base}?error=" + quote(f"导入失败：{exc}"), status_code=303)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _import_with_categories(
    src: sqlite3.Connection,
    db: Session,
) -> tuple[list[MonitorTask], int, int]:
    """Import from a new-format .db that contains both categories and monitor_tasks."""
    # 1. Read source categories and build old_id -> name mapping.
    src_categories = src.execute("SELECT id, name FROM categories").fetchall()

    # Check which columns exist in monitor_tasks to handle both old and new schemas.
    src_columns = {row[1] for row in src.execute("PRAGMA table_info(monitor_tasks)")}
    has_category_id = "category_id" in src_columns

    # 2. Create or reuse groups in current DB.
    existing_groups = {g.name: g for g in db.query(Category).all()}
    old_id_to_new_group: dict[int, Category] = {}
    groups_created = 0

    for row in src_categories:
        cat_name = (row["name"] or "").strip()
        if not cat_name:
            continue
        if cat_name in existing_groups:
            old_id_to_new_group[row["id"]] = existing_groups[cat_name]
        else:
            new_group = Category(name=cat_name)
            db.add(new_group)
            db.flush()  # get the new id
            existing_groups[cat_name] = new_group
            old_id_to_new_group[row["id"]] = new_group
            groups_created += 1

    # 3. Read source tasks.
    cols = "name, url, selector, check_interval_hours, recipients, is_active"
    if has_category_id:
        cols += ", category_id"
    rows = src.execute(f"SELECT {cols} FROM monitor_tasks").fetchall()

    existing_urls = {r[0] for r in db.query(MonitorTask.url).all()}
    existing_names = {r[0] for r in db.query(MonitorTask.name).all()}
    imported: list[MonitorTask] = []
    skipped = 0

    for row in rows:
        raw_url = (row["url"] or "").strip()
        if not raw_url or raw_url in existing_urls:
            skipped += 1
            continue
        try:
            validated_url = validate_monitor_target_url(raw_url)
            recipients_val = normalize_recipients(row["recipients"] or "")
        except ValueError:
            skipped += 1
            continue

        task_name = (row["name"] or "").strip() or validated_url
        # Ensure name uniqueness.
        if task_name in existing_names:
            skipped += 1
            continue

        interval = max(1, min(168, int(row["check_interval_hours"] or 24)))

        # Resolve target group from the source category_id.
        new_group_id = None
        if has_category_id and row["category_id"]:
            group = old_id_to_new_group.get(row["category_id"])
            if group:
                new_group_id = group.id

        # If task has no group, put it in an "未分组" fallback group.
        if not new_group_id:
            fallback_name = "未分组"
            if fallback_name not in existing_groups:
                fallback = Category(name=fallback_name)
                db.add(fallback)
                db.flush()
                existing_groups[fallback_name] = fallback
                groups_created += 1
            new_group_id = existing_groups[fallback_name].id

        task = MonitorTask(
            name=task_name,
            url=validated_url,
            selector=(row["selector"] or "div[data-component-type='s-search-result']").strip(),
            check_interval_hours=interval,
            recipients=recipients_val,
            category_id=new_group_id,
            is_active=bool(row["is_active"]),
        )
        db.add(task)
        existing_urls.add(validated_url)
        existing_names.add(task_name)
        imported.append(task)

    return imported, skipped, groups_created


def _import_legacy_to_pending(
    src: sqlite3.Connection,
    db: Session,
) -> tuple[int, int]:
    """Import from an old-format .db into the pending_imports table for manual assignment."""
    rows = src.execute(
        "SELECT name, url, selector, check_interval_hours, recipients, is_active FROM monitor_tasks"
    ).fetchall()

    existing_urls = {r[0] for r in db.query(MonitorTask.url).all()}
    existing_names = {r[0] for r in db.query(MonitorTask.name).all()}
    pending_urls = {r[0] for r in db.query(PendingImport.url).all()}
    added = 0
    skipped = 0

    for row in rows:
        raw_url = (row["url"] or "").strip()
        if not raw_url or raw_url in existing_urls or raw_url in pending_urls:
            skipped += 1
            continue
        try:
            validated_url = validate_monitor_target_url(raw_url)
            recipients_val = normalize_recipients(row["recipients"] or "")
        except ValueError:
            skipped += 1
            continue

        task_name = (row["name"] or "").strip() or validated_url
        if task_name in existing_names:
            skipped += 1
            continue

        interval = max(1, min(168, int(row["check_interval_hours"] or 24)))
        pending = PendingImport(
            name=task_name,
            url=validated_url,
            selector=(row["selector"] or "div[data-component-type='s-search-result']").strip(),
            check_interval_hours=interval,
            recipients=recipients_val,
            is_active=bool(row["is_active"]),
        )
        db.add(pending)
        pending_urls.add(validated_url)
        added += 1

    return added, skipped


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
    total_pages = max(1, (total + _TASKS_PAGE_SIZE - 1) // _TASKS_PAGE_SIZE)
    page = min(page, total_pages)
    tasks = q.offset((page - 1) * _TASKS_PAGE_SIZE).limit(_TASKS_PAGE_SIZE).all()

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

def _task_redirect(task: MonitorTask, extra: str = "") -> RedirectResponse:
    gid = task.category_id
    base = f"/tasks/group/{gid}" if gid else "/tasks"
    url = f"{base}?{extra}" if extra else base
    return RedirectResponse(url=url, status_code=303)


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
    
    # Calculate which page the new task is on and redirect there
    total_tasks = db.query(MonitorTask).filter(MonitorTask.category_id == group_id).count()
    last_page = (total_tasks + _TASKS_PAGE_SIZE - 1) // _TASKS_PAGE_SIZE
    return RedirectResponse(
        url=f"/tasks/group/{group_id}?page={last_page}&highlight={new_task.id}",
        status_code=303
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
        url=f"/tasks/group/{source_group_id}?batch_moved={moved}&target_name={quote(target_group.name)}",
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
        url=f"/tasks/group/{source_group_id}?batch_deleted={deleted}",
        status_code=303,
    )


@router.post("/tasks/batch-update-interval")
async def batch_update_interval(
    request: Request,
    db: Session = Depends(get_db),
):
    """Batch update check interval for selected tasks."""
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
            # Re-schedule the task with new interval
            remove_scheduled_task(task.id)
            schedule_task(task)
            updated += 1
    
    db.commit()
    return RedirectResponse(
        url=f"/tasks/group/{source_group_id}?batch_updated={updated}&new_interval={new_interval}",
        status_code=303,
    )


@router.post("/tasks/group/{group_id}/claim-pending")
async def claim_pending_imports(
    group_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """Move selected pending imports into a group as real monitor tasks."""
    group = db.query(Category).filter(Category.id == group_id).first()
    if not group:
        return RedirectResponse(url="/tasks?error=" + quote("分组不存在。"), status_code=303)

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

    # Schedule newly created tasks.
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
        url=f"/tasks/group/{group_id}?claimed={claimed}",
        status_code=303,
    )


@router.post("/tasks/clear-pending")
async def clear_pending_imports(db: Session = Depends(get_db)):
    """Delete all pending imports."""
    db.query(PendingImport).delete()
    db.commit()
    return RedirectResponse(url="/tasks?pending_cleared=1", status_code=303)


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
    """Reset consecutive empty count and health state to healthy."""
    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
    if task:
        task.consecutive_empty_count = 0
        task.health_state = "healthy"
        db.commit()
    return RedirectResponse(url="/", status_code=303)


# ---------------------------------------------------------------------------
# Queue status API (polled by frontend)
# ---------------------------------------------------------------------------

@router.get("/api/queue-status")
async def queue_status(db: Session = Depends(get_db)):
    running_id, waiting_ids = get_queue_snapshot()
    items: list[dict] = []

    if running_id:
        task = db.query(MonitorTask).filter(MonitorTask.id == running_id).first()
        items.append({"id": running_id, "name": task.name if task else f"Task-{running_id}", "status": "running"})

    for tid in waiting_ids:
        task = db.query(MonitorTask).filter(MonitorTask.id == tid).first()
        items.append({"id": tid, "name": task.name if task else f"Task-{tid}", "status": "waiting"})

    return JSONResponse({"items": items, "total": len(items)})


# ---------------------------------------------------------------------------
# Network Check API (test if Amazon is accessible)
# ---------------------------------------------------------------------------

@router.get("/api/network-check")
async def network_check():
    """Check if Amazon is accessible and measure response time."""
    import time
    import requests
    import asyncio
    
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
            try:
                start = time.time()
                resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
                elapsed = (time.time() - start) * 1000
                total_time += elapsed
                
                status = resp.status_code
                if status == 200:
                    content = resp.text[:2000].lower()
                    if "captcha" in content or ("robot" in content and "robots.txt" not in url):
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
            except requests.Timeout:
                results["details"].append({
                    "name": name,
                    "status": "❌ 超时",
                    "time_ms": None,
                    "note": "连接超时 (>15s)"
                })
                all_ok = False
            except Exception as e:
                results["details"].append({
                    "name": name,
                    "status": "❌ 失败",
                    "time_ms": None,
                    "note": str(e)[:50]
                })
                all_ok = False
        
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
    
    # Run blocking code in thread pool
    import asyncio
    results = await asyncio.to_thread(_do_check)
    return JSONResponse(results)


# ---------------------------------------------------------------------------
# Settings / Setup  (unchanged)
# ---------------------------------------------------------------------------

@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, db: Session = Depends(get_db)):
    config = db.query(SystemConfig).first()
    if not config:
        config = SystemConfig()
        db.add(config)
        db.commit()
        db.refresh(config)
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "config": config,
            "has_sender_password": bool(config.sender_password),
        },
    )


@public_router.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request, db: Session = Depends(get_db)):
    config = db.query(SystemConfig).first()
    if config and config.setup_complete:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("setup.html", {"request": request})


@public_router.post("/setup", response_class=HTMLResponse)
async def complete_setup(
    request: Request,
    admin_password: str = Form(...),
    smtp_server: str = Form(...),
    smtp_port: int = Form(...),
    sender_email: str = Form(...),
    sender_password: str = Form(...),
    db: Session = Depends(get_db),
):
    def _err(msg: str):
        return templates.TemplateResponse("setup.html", {"request": request, "error": msg}, status_code=400)

    if len(admin_password.strip()) < 6:
        return _err("密码至少需要 6 位。")
    if not smtp_server.strip():
        return _err("SMTP 服务器不能为空。")
    if smtp_port < 1 or smtp_port > 65535:
        return _err("SMTP 端口无效。")
    sender = sender_email.strip()
    if not sender:
        return _err("发件邮箱不能为空。")
    if not sender_password.strip():
        return _err("邮箱授权码不能为空。")

    try:
        enc_admin = encrypt_secret(admin_password)
        enc_smtp = encrypt_secret(sender_password)
    except RuntimeError as exc:
        return _err(str(exc))

    config = db.query(SystemConfig).first()
    if not config:
        config = SystemConfig()
        db.add(config)

    config.admin_password_enc = enc_admin
    config.smtp_server = smtp_server.strip()
    config.smtp_port = smtp_port
    config.sender_email = sender
    config.sender_password = enc_smtp
    config.setup_complete = True
    db.commit()
    return RedirectResponse(url="/", status_code=303)


@router.post("/settings")
async def update_settings(
    smtp_server: str = Form(...),
    smtp_port: int = Form(...),
    sender_email: str = Form(...),
    sender_password: str = Form(""),
    product_retention_days: int = Form(90),
    proxy_url: str = Form(""),
    db: Session = Depends(get_db),
):
    smtp_host = smtp_server.strip()
    sender = sender_email.strip()
    if not smtp_host:
        raise HTTPException(status_code=400, detail="SMTP server cannot be empty.")
    if smtp_port < 1 or smtp_port > 65535:
        raise HTTPException(status_code=400, detail="SMTP port must be between 1 and 65535.")
    if not sender:
        raise HTTPException(status_code=400, detail="Sender email cannot be empty.")
    if product_retention_days < 7 or product_retention_days > 365:
        raise HTTPException(status_code=400, detail="Product retention days must be between 7 and 365.")

    config = db.query(SystemConfig).first()
    if not config:
        config = SystemConfig()
        db.add(config)

    config.smtp_server = smtp_host
    config.smtp_port = smtp_port
    config.sender_email = sender
    config.product_retention_days = product_retention_days
    config.proxy_url = proxy_url.strip()
    if sender_password.strip():
        try:
            config.sender_password = encrypt_secret(sender_password)
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    db.commit()
    # Apply proxy immediately for current process.
    proxy = config.proxy_url or ""
    if proxy:
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy
    else:
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("HTTPS_PROXY", None)
    return RedirectResponse(url="/settings?success=1", status_code=303)
