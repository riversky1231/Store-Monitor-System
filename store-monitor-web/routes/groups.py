"""Group management routes."""
import os
import sqlite3
import tempfile

from fastapi import APIRouter, Depends, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func as sa_func
from sqlalchemy.orm import Session

from database import get_db
from models import Category, MonitorTask, PendingImport
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
    normalize_recipients,
    require_admin_auth,
    validate_monitor_target_url,
)

from . import templates
from ._shared import _build_url

router = APIRouter(dependencies=[Depends(require_admin_auth)])

_IMPORT_MAX_BYTES = 50 * 1024 * 1024  # 50 MB
_SQLITE_MAGIC = b"SQLite format 3\x00"


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


# ---------------------------------------------------------------------------
# Group CRUD
# ---------------------------------------------------------------------------

@router.post("/tasks/group-create")
async def create_group(name: str = Form(...), db: Session = Depends(get_db)):
    name = name.strip()
    if not name:
        return RedirectResponse(url=_build_url("/tasks", error="分组名称不能为空。"), status_code=303)
    existing = db.query(Category).filter(Category.name == name).first()
    if existing:
        return RedirectResponse(url=_build_url("/tasks", error=f"分组「{name}」已存在。"), status_code=303)
    db.add(Category(name=name))
    db.commit()
    return RedirectResponse(url=_build_url("/tasks", created=1), status_code=303)


@router.post("/tasks/group/{group_id}/edit")
async def edit_group(group_id: int, name: str = Form(...), db: Session = Depends(get_db)):
    name = name.strip()
    if not name:
        return RedirectResponse(
            url=_build_url(f"/tasks/group/{group_id}", error="分组名称不能为空。"),
            status_code=303,
        )
    group = db.query(Category).filter(Category.id == group_id).first()
    if not group:
        return RedirectResponse(url="/tasks", status_code=303)
    duplicate = db.query(Category).filter(Category.name == name, Category.id != group_id).first()
    if duplicate:
        return RedirectResponse(
            url=_build_url(f"/tasks/group/{group_id}", error=f"分组「{name}」已存在。"),
            status_code=303,
        )
    group.name = name
    db.commit()
    return RedirectResponse(url=_build_url(f"/tasks/group/{group_id}", updated=1), status_code=303)


@router.post("/tasks/group/{group_id}/delete")
async def delete_group(group_id: int, db: Session = Depends(get_db)):
    group = db.query(Category).filter(Category.id == group_id).first()
    if group:
        for task in group.tasks:
            remove_scheduled_task(task.id)
        db.delete(group)
        db.commit()
    return RedirectResponse(url="/tasks", status_code=303)


@router.post("/tasks/run-all")
async def run_all_tasks(db: Session = Depends(get_db)):
    tasks = db.query(MonitorTask).filter(MonitorTask.is_active.is_(True)).all()
    queued = 0
    for task in tasks:
        if queue_monitor_task(task.id):
            queued += 1
    return RedirectResponse(url=_build_url("/tasks", run_all=1, queued=queued), status_code=303)


@router.post("/tasks/queue/stop")
async def stop_queue():
    running_id, cleared = force_stop_queue()
    return RedirectResponse(
        url=_build_url("/tasks", queue_stopped=1, cleared=cleared, running=running_id),
        status_code=303,
    )


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

    target_group_id = None
    if target_group_id_raw:
        try:
            target_group_id = int(target_group_id_raw)
        except (ValueError, TypeError):
            pass

    redirect_base = "/tasks"
    filename = db_file.filename or ""
    if not filename.lower().endswith(".db"):
        return RedirectResponse(
            url=_build_url(redirect_base, error="请上传 .db 格式的数据库文件。"),
            status_code=303,
        )

    content = await db_file.read()
    if len(content) > _IMPORT_MAX_BYTES:
        return RedirectResponse(
            url=_build_url(redirect_base, error="文件过大，最大支持 50 MB。"),
            status_code=303,
        )
    if len(content) < 100 or not content.startswith(_SQLITE_MAGIC):
        return RedirectResponse(
            url=_build_url(redirect_base, error="文件不是有效的 SQLite 数据库。"),
            status_code=303,
        )

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
            return RedirectResponse(
                url=_build_url(redirect_base, error="数据库中未找到 monitor_tasks 表。"),
                status_code=303,
            )

        has_categories = "categories" in tables

        if has_categories:
            result = _import_with_categories(src, db)
            src.close()
            imported, skipped, groups_created = result
            db.commit()
            for task in imported:
                db.refresh(task)
                schedule_task(task)
            return RedirectResponse(
                url=_build_url(
                    redirect_base,
                    import_ok=1,
                    imported=len(imported),
                    skipped=skipped,
                    groups=groups_created,
                ),
                status_code=303,
            )
        else:
            pending_count, skipped = _import_legacy_to_pending(src, db)
            src.close()
            db.commit()
            return RedirectResponse(
                url=_build_url(
                    redirect_base,
                    pending_ok=1,
                    pending=pending_count,
                    skipped=skipped,
                ),
                status_code=303,
            )

    except Exception as exc:
        return RedirectResponse(
            url=_build_url(redirect_base, error=f"导入失败：{exc}"),
            status_code=303,
        )
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _import_with_categories(
    src: sqlite3.Connection,
    db: Session,
) -> tuple[list[MonitorTask], int, int]:
    """Import from a new-format .db that contains both categories and monitor_tasks."""
    src_categories = src.execute("SELECT id, name FROM categories").fetchall()
    src_columns = {row[1] for row in src.execute("PRAGMA table_info(monitor_tasks)")}
    has_category_id = "category_id" in src_columns

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
            db.flush()
            existing_groups[cat_name] = new_group
            old_id_to_new_group[row["id"]] = new_group
            groups_created += 1

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
        if task_name in existing_names:
            skipped += 1
            continue

        interval = max(1, min(168, int(row["check_interval_hours"] or 24)))

        new_group_id = None
        if has_category_id and row["category_id"]:
            group = old_id_to_new_group.get(row["category_id"])
            if group:
                new_group_id = group.id

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
