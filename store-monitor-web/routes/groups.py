"""Group management routes."""
import os
import sqlite3
import tempfile
import logging
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func as sa_func, or_
from sqlalchemy.orm import Session

from database import get_db
from models import Category, MonitorTask, PendingImport
from scheduler import (
    build_group_digest_scope,
    force_stop_queue,
    get_queue_snapshot,
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
from ._shared import _redirect_back

router = APIRouter(dependencies=[Depends(require_admin_auth)])
logger = logging.getLogger(__name__)

_IMPORT_MAX_BYTES = 50 * 1024 * 1024  # 50 MB
_IMPORT_CHUNK_BYTES = 1024 * 1024
_SQLITE_MAGIC = b"SQLite format 3\x00"


class _UploadTooLarge(ValueError):
    pass


class _InvalidSqliteUpload(ValueError):
    pass


async def _stream_upload_to_temp_db(db_file: UploadFile) -> str:
    """Copy an upload to a temp file without ever reading beyond the size cap."""
    tmp_path = ""
    total_bytes = 0
    header = bytearray()
    try:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name
            while True:
                chunk = await db_file.read(_IMPORT_CHUNK_BYTES)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > _IMPORT_MAX_BYTES:
                    raise _UploadTooLarge
                if len(header) < len(_SQLITE_MAGIC):
                    needed = len(_SQLITE_MAGIC) - len(header)
                    header.extend(chunk[:needed])
                tmp.write(chunk)

        if total_bytes < 100 or bytes(header) != _SQLITE_MAGIC:
            raise _InvalidSqliteUpload
        return tmp_path
    except Exception:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def _table_column_names(src: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[1] for row in src.execute(f"PRAGMA table_info({table_name})")}


def _row_value(row: sqlite3.Row, column: str, default=None):
    if column not in row.keys():
        return default
    value = row[column]
    return default if value is None else value


def _row_bool_value(row: sqlite3.Row, column: str, default: bool = True) -> bool:
    value = _row_value(row, column, default)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"0", "false", "no", "off", ""}:
            return False
        if normalized in {"1", "true", "yes", "on"}:
            return True
    return bool(value)


def _task_issue_summary(task: MonitorTask, running_id: int | None, waiting_ids: set[int]) -> dict:
    if task.manual_review_pending:
        status_text = "待人工确认"
    elif task.health_state == "failed":
        status_text = f"失败 {task.consecutive_failure_count or 0} 次"
    elif task.health_state == "alert":
        status_text = f"空抓告警 {task.consecutive_empty_count or 0} 次"
    elif task.health_state == "warning":
        if task.consecutive_failure_count:
            status_text = f"异常 {task.consecutive_failure_count} 次"
        else:
            status_text = f"空抓预警 {task.consecutive_empty_count or 0} 次"
    else:
        status_text = "正常"

    if task.id == running_id:
        execution_state = "running"
    elif task.id in waiting_ids:
        execution_state = "waiting"
    elif task.manual_review_pending:
        execution_state = "manual"
    else:
        execution_state = "idle"

    return {
        "id": task.id,
        "name": task.name,
        "group_id": task.category_id,
        "group_name": task.group.name if task.group else "未分组",
        "group_url": _task_group_focus_url(task),
        "task": task,
        "status_text": status_text,
        "reason": task.last_failure_reason or "",
        "execution_state": execution_state,
        "updated_at": task.last_failure_at or task.last_health_alert_at or task.last_run_at,
    }


def _task_has_pending_issue(task: MonitorTask, active_queue_ids: set[int]) -> bool:
    if not task.is_active:
        return False
    if task.id in active_queue_ids:
        return False
    return (
        task.manual_review_pending
        or
        task.health_state in {"warning", "alert", "failed"}
        or (task.consecutive_failure_count or 0) > 0
    )


def _current_path_query(request: Request) -> str:
    query = request.url.query
    return f"{request.url.path}?{query}" if query else request.url.path


def _task_group_focus_url(task: MonitorTask) -> str:
    if not task.category_id:
        return ""
    query = urlencode({
        "search": task.name,
        "highlight": task.id,
    })
    return f"/tasks/group/{task.category_id}?{query}"


@router.get("/tasks/search-store")
async def search_store_location(
    q: str = Query(""),
    db: Session = Depends(get_db),
):
    """Locate a task from the overview and jump directly to its group row."""
    query_text = (q or "").strip()
    if not query_text:
        return RedirectResponse(url="/tasks", status_code=303)

    pattern = f"%{query_text}%"
    matches = (
        db.query(MonitorTask)
        .outerjoin(Category, MonitorTask.category_id == Category.id)
        .filter(
            or_(
                MonitorTask.name.ilike(pattern),
                MonitorTask.url.ilike(pattern),
                Category.name.ilike(pattern),
            )
        )
        .order_by(MonitorTask.name.asc(), MonitorTask.id.asc())
        .all()
    )
    logger.info("External task location search: query_length=%d matches=%d", len(query_text), len(matches))
    if len(matches) == 1 and matches[0].category_id:
        return RedirectResponse(url=_task_group_focus_url(matches[0]), status_code=303)

    params = {"locate": query_text}
    if not matches:
        params["locate_error"] = "1"
    return RedirectResponse(url=f"/tasks?{urlencode(params)}", status_code=303)


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
    running_id, waiting_ids = get_queue_snapshot()
    waiting_set = set(waiting_ids)
    active_queue_ids = set(waiting_set)
    if running_id is not None:
        active_queue_ids.add(running_id)
    tasks = db.query(MonitorTask).all()
    abnormal_tasks = []
    failed_tasks = []
    for task in tasks:
        if not _task_has_pending_issue(task, active_queue_ids):
            continue
        summary = _task_issue_summary(task, running_id, waiting_set)
        if task.health_state == "failed" and not task.manual_review_pending:
            failed_tasks.append(summary)
        else:
            abnormal_tasks.append(summary)

    abnormal_tasks.sort(key=lambda item: (item["execution_state"] != "running", item["execution_state"] != "waiting", item["name"]))
    failed_tasks.sort(key=lambda item: (item["execution_state"] != "running", item["name"]))

    locate_query = (request.query_params.get("locate") or "").strip()
    location_results = []
    if locate_query:
        pattern = f"%{locate_query}%"
        location_results = (
            db.query(MonitorTask)
            .outerjoin(Category, MonitorTask.category_id == Category.id)
            .filter(
                or_(
                    MonitorTask.name.ilike(pattern),
                    MonitorTask.url.ilike(pattern),
                    Category.name.ilike(pattern),
                )
            )
            .order_by(MonitorTask.name.asc(), MonitorTask.id.asc())
            .limit(20)
            .all()
        )

    return templates.TemplateResponse(request, "groups.html", {
        "request": request,
        "groups": groups,
        "task_counts": task_counts,
        "search": search,
        "pending_count": pending_count,
        "abnormal_tasks": abnormal_tasks[:20],
        "failed_tasks": failed_tasks[:20],
        "current_path_query": _current_path_query(request),
        "location_results": location_results,
        "locate_query": locate_query,
        "task_focus_url": _task_group_focus_url,
    })


# ---------------------------------------------------------------------------
# Group CRUD
# ---------------------------------------------------------------------------

@router.post("/tasks/group-create")
async def create_group(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    name = (form.get("name") or "").strip()
    next_url = (form.get("next") or "").strip()
    if not name:
        return _redirect_back(next_url, "/tasks", error="分组名称不能为空。")
    existing = db.query(Category).filter(Category.name == name).first()
    if existing:
        return _redirect_back(next_url, "/tasks", error=f"分组「{name}」已存在。")
    db.add(Category(name=name))
    db.commit()
    return _redirect_back(next_url, "/tasks", created=1)


@router.post("/tasks/group/{group_id}/edit")
@router.post("/tasks/group/{group_id}/rename")
async def edit_group(group_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    name = (form.get("name") or "").strip()
    next_url = (form.get("next") or "").strip()
    if not name:
        return _redirect_back(next_url, "/tasks", error="分组名称不能为空。")
    group = db.query(Category).filter(Category.id == group_id).first()
    if not group:
        return _redirect_back(next_url, "/tasks")
    duplicate = db.query(Category).filter(Category.name == name, Category.id != group_id).first()
    if duplicate:
        return _redirect_back(next_url, "/tasks", error=f"分组「{name}」已存在。")
    group.name = name
    db.commit()
    return _redirect_back(next_url, "/tasks", updated=1)


@router.post("/tasks/group/{group_id}/delete")
async def delete_group(group_id: int, request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    group = db.query(Category).filter(Category.id == group_id).first()
    if group:
        resolved_reviews = [
            (task.manual_review_scope_key, task.id)
            for task in group.tasks
            if task.manual_review_pending
        ]
        for task in group.tasks:
            remove_scheduled_task(task.id)
        db.delete(group)
        db.commit()
        for scope_key, task_id in resolved_reviews:
            resolve_manual_review_report(
                scope_key,
                task_id,
                resolution_message="人工处理：分组已删除",
            )
    return _redirect_back(next_url, "/tasks")


@router.post("/tasks/run-all")
async def run_all_tasks(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    tasks = (
        db.query(MonitorTask)
        .filter(
            MonitorTask.is_active.is_(True),
            or_(
                MonitorTask.manual_review_pending.is_(False),
                MonitorTask.manual_review_pending.is_(None),
            ),
        )
        .order_by(
            MonitorTask.category_id.asc(),
            MonitorTask.sort_order.asc(),
            MonitorTask.id.asc(),
        )
        .all()
    )
    tasks_by_group: dict[tuple[int | None, str], list[int]] = {}
    for task in tasks:
        group_name = task.group.name if task.group else "未分组"
        tasks_by_group.setdefault((task.category_id, group_name), []).append(task.id)

    queued = 0
    reports = 0
    for (group_id, group_name), task_ids in tasks_by_group.items():
        digest_scope = build_group_digest_scope(group_id, group_name, unique_run=True)
        group_queued = queue_monitor_batch(task_ids, digest_scope)
        queued += group_queued
        if group_queued:
            reports += 1
    return _redirect_back(next_url, "/tasks", run_all=1, queued=queued, reports=reports)


@router.post("/tasks/queue/stop")
async def stop_queue(request: Request):
    form = await request.form()
    next_url = (form.get("next") or "").strip()
    running_id, cleared = force_stop_queue()
    return _redirect_back(next_url, "/tasks", queue_stopped=1, cleared=cleared, running=running_id)


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
    next_url = (form.get("next") or "").strip()
    db_file: UploadFile = form["db_file"]

    target_group_id = None
    if target_group_id_raw:
        try:
            target_group_id = int(target_group_id_raw)
        except (ValueError, TypeError):
            pass

    redirect_base = "/tasks"
    tmp_path = None
    src = None
    try:
        filename = db_file.filename or ""
        if not filename.lower().endswith(".db"):
            return _redirect_back(next_url, redirect_base, error="请上传 .db 格式的数据库文件。")

        try:
            tmp_path = await _stream_upload_to_temp_db(db_file)
        except _UploadTooLarge:
            return _redirect_back(next_url, redirect_base, error="文件过大，最大支持 50 MB。")
        except _InvalidSqliteUpload:
            return _redirect_back(next_url, redirect_base, error="文件不是有效的 SQLite 数据库。")

        src = sqlite3.connect(tmp_path)
        src.row_factory = sqlite3.Row
        tables = {r[0] for r in src.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "monitor_tasks" not in tables:
            return _redirect_back(next_url, redirect_base, error="数据库中未找到 monitor_tasks 表。")

        has_categories = "categories" in tables

        if has_categories:
            result = _import_with_categories(src, db)
            imported, skipped, groups_created = result
            db.commit()
            for task in imported:
                db.refresh(task)
                schedule_task(task)
            return _redirect_back(
                next_url,
                redirect_base,
                import_ok=1,
                imported=len(imported),
                skipped=skipped,
                groups=groups_created,
            )
        else:
            pending_count, skipped = _import_legacy_to_pending(src, db)
            db.commit()
            return _redirect_back(
                next_url,
                redirect_base,
                pending_ok=1,
                pending=pending_count,
                skipped=skipped,
            )

    except sqlite3.Error as exc:
        logger.warning("SQLite import failed: %s", exc)
        return _redirect_back(next_url, redirect_base, error="导入失败：源数据库结构不兼容或已损坏。")
    except Exception as exc:
        logger.exception("Task import failed: %s", exc)
        return _redirect_back(next_url, redirect_base, error="导入失败：处理文件时发生异常。")
    finally:
        if src is not None:
            src.close()
        try:
            await db_file.close()
        except Exception:
            logger.debug("Failed to close imported upload.", exc_info=True)
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _import_with_categories(
    src: sqlite3.Connection,
    db: Session,
) -> tuple[list[MonitorTask], int, int]:
    """Import from a new-format .db that contains both categories and monitor_tasks."""
    src_categories = src.execute("SELECT id, name FROM categories").fetchall()
    src_columns = _table_column_names(src, "monitor_tasks")
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

    cols = ["name", "url"]
    if "selector" in src_columns:
        cols.append("selector")
    if "check_interval_hours" in src_columns:
        cols.append("check_interval_hours")
    if "recipients" in src_columns:
        cols.append("recipients")
    if "is_active" in src_columns:
        cols.append("is_active")
    if has_category_id:
        cols.append("category_id")
    rows = src.execute(f"SELECT {', '.join(cols)} FROM monitor_tasks").fetchall()

    existing_urls = {r[0] for r in db.query(MonitorTask.url).all()}
    existing_names = {r[0] for r in db.query(MonitorTask.name).all()}
    imported: list[MonitorTask] = []
    skipped = 0
    next_sort_by_group: dict[int, int] = {}

    for row in rows:
        raw_url = str(_row_value(row, "url", "")).strip()
        if not raw_url or raw_url in existing_urls:
            skipped += 1
            continue
        try:
            validated_url = validate_monitor_target_url(raw_url)
            recipients_val = normalize_recipients(str(_row_value(row, "recipients", "")) or "")
        except ValueError:
            skipped += 1
            continue

        task_name = str(_row_value(row, "name", "")).strip() or validated_url
        if task_name in existing_names:
            skipped += 1
            continue

        interval = max(1, min(168, int(_row_value(row, "check_interval_hours", 24) or 24)))

        new_group_id = None
        category_id = _row_value(row, "category_id")
        if has_category_id and category_id:
            group = old_id_to_new_group.get(category_id)
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

        if new_group_id not in next_sort_by_group:
            maximum = (
                db.query(sa_func.max(MonitorTask.sort_order))
                .filter(MonitorTask.category_id == new_group_id)
                .scalar()
            )
            next_sort_by_group[new_group_id] = int(maximum) + 1 if maximum is not None else 0

        task = MonitorTask(
            name=task_name,
            url=validated_url,
            selector=str(_row_value(row, "selector", "div[data-component-type='s-search-result']")).strip(),
            check_interval_hours=interval,
            recipients=recipients_val,
            category_id=new_group_id,
            sort_order=next_sort_by_group[new_group_id],
            is_active=_row_bool_value(row, "is_active", True),
        )
        db.add(task)
        next_sort_by_group[new_group_id] += 1
        existing_urls.add(validated_url)
        existing_names.add(task_name)
        imported.append(task)

    return imported, skipped, groups_created


def _import_legacy_to_pending(
    src: sqlite3.Connection,
    db: Session,
) -> tuple[int, int]:
    """Import from an old-format .db into the pending_imports table for manual assignment."""
    src_columns = _table_column_names(src, "monitor_tasks")
    cols = ["name", "url"]
    if "selector" in src_columns:
        cols.append("selector")
    if "check_interval_hours" in src_columns:
        cols.append("check_interval_hours")
    if "recipients" in src_columns:
        cols.append("recipients")
    if "is_active" in src_columns:
        cols.append("is_active")
    rows = src.execute(f"SELECT {', '.join(cols)} FROM monitor_tasks").fetchall()

    existing_urls = {r[0] for r in db.query(MonitorTask.url).all()}
    existing_names = {r[0] for r in db.query(MonitorTask.name).all()}
    pending_urls = {r[0] for r in db.query(PendingImport.url).all()}
    added = 0
    skipped = 0

    for row in rows:
        raw_url = str(_row_value(row, "url", "")).strip()
        if not raw_url or raw_url in existing_urls or raw_url in pending_urls:
            skipped += 1
            continue
        try:
            validated_url = validate_monitor_target_url(raw_url)
            recipients_val = normalize_recipients(str(_row_value(row, "recipients", "")) or "")
        except ValueError:
            skipped += 1
            continue

        task_name = str(_row_value(row, "name", "")).strip() or validated_url
        if task_name in existing_names:
            skipped += 1
            continue

        interval = max(1, min(168, int(_row_value(row, "check_interval_hours", 24) or 24)))
        pending = PendingImport(
            name=task_name,
            url=validated_url,
            selector=str(_row_value(row, "selector", "div[data-component-type='s-search-result']")).strip(),
            check_interval_hours=interval,
            recipients=recipients_val,
            is_active=_row_bool_value(row, "is_active", True),
        )
        db.add(pending)
        pending_urls.add(validated_url)
        added += 1

    return added, skipped
