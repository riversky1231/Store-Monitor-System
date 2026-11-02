import json
import sqlite3
import tempfile
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient

import scraper
from models import Category, MonitorTask, PendingImport
from routes.groups import _import_legacy_to_pending
from routes.groups import router as groups_router
from routes.tasks import router as tasks_router


def test_groups_page_escapes_error_query_param(app_factory):
    app = app_factory(groups_router)
    client = TestClient(app)

    response = client.get("/tasks?error=%3Cscript%3Ealert(1)%3C%2Fscript%3E")

    assert response.status_code == 200
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in response.text
    assert '<div class="alert alert-err"><script>alert(1)</script></div>' not in response.text


def test_group_import_rejects_oversized_upload_while_streaming(app_factory, monkeypatch):
    monkeypatch.setattr("routes.groups._IMPORT_MAX_BYTES", 32)
    app = app_factory(groups_router)
    client = TestClient(app)

    response = client.post(
        "/tasks/group-import",
        files={"db_file": ("too-large.db", b"SQLite format 3\x00" + b"x" * 64, "application/octet-stream")},
        follow_redirects=False,
    )

    assert response.status_code == 303
    query = parse_qs(urlparse(response.headers["location"]).query)
    assert query["error"] == ["文件过大，最大支持 50 MB。"]


def test_group_page_escapes_target_name_query_param(app_factory, session_factory, monkeypatch):
    monkeypatch.setattr("routes.tasks.get_inflight_task_ids", lambda: set())

    with session_factory() as db:
        group = Category(name="源分组")
        db.add(group)
        db.commit()
        db.refresh(group)
        group_id = group.id

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.get(
        f"/tasks/group/{group_id}?batch_moved=1&target_name=%3Cimg%20src%3Dx%20onerror%3Dalert(1)%3E"
    )

    assert response.status_code == 200
    assert "&lt;img src=x onerror=alert(1)&gt;" in response.text
    assert "onerror=alert(1)" in response.text
    assert '「<img src=x onerror=alert(1)>」' not in response.text


def test_batch_move_redirect_uses_encoded_target_group_name(app_factory, session_factory):
    with session_factory() as db:
        source = Category(name="源分组")
        target = Category(name="A&B/新区")
        db.add_all([source, target])
        db.commit()
        db.refresh(source)
        db.refresh(target)

        task = MonitorTask(
            name="Store 1",
            url="https://example.com/store-1",
            selector="div.item",
            check_interval_hours=24,
            recipients="a@example.com",
            category_id=source.id,
            is_active=True,
        )
        db.add(task)
        db.commit()
        db.refresh(task)

        source_id = source.id
        target_id = target.id
        task_id = task.id

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(
        "/tasks/batch-move",
        data={
            "task_ids": [str(task_id)],
            "target_group_id": str(target_id),
            "source_group_id": str(source_id),
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert (
        response.headers["location"]
        == f"/tasks/group/{source_id}?batch_moved=1&target_name=A%26B%2F%E6%96%B0%E5%8C%BA"
    )


def test_run_group_queues_active_tasks_with_group_scope(app_factory, session_factory, monkeypatch):
    with session_factory() as db:
        group = Category(name="电子产品")
        db.add(group)
        db.commit()
        db.refresh(group)

        tasks = [
            MonitorTask(
                name="Store A",
                url="https://example.com/a",
                selector="div.item",
                check_interval_hours=24,
                recipients="a@example.com",
                category_id=group.id,
                is_active=True,
            ),
            MonitorTask(
                name="Store B",
                url="https://example.com/b",
                selector="div.item",
                check_interval_hours=24,
                recipients="b@example.com",
                category_id=group.id,
                is_active=True,
            ),
            MonitorTask(
                name="Store C",
                url="https://example.com/c",
                selector="div.item",
                check_interval_hours=24,
                recipients="c@example.com",
                category_id=group.id,
                is_active=False,
            ),
        ]
        db.add_all(tasks)
        db.commit()
        db.refresh(group)
        active_task_ids = [tasks[0].id, tasks[1].id]

    queued_calls = []

    def _fake_queue(task_ids, digest_scope):
        queued_calls.append((task_ids, digest_scope))
        return len(task_ids)

    monkeypatch.setattr("routes.tasks.queue_monitor_batch", _fake_queue)

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(f"/tasks/group/{group.id}/run", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == f"/tasks/group/{group.id}?run_group=1&queued=2"
    assert len(queued_calls) == 1
    assert set(queued_calls[0][0]) == set(active_task_ids)
    assert queued_calls[0][1]["key"].startswith(f"group:{group.id}:run:")
    assert queued_calls[0][1]["label"] == "电子产品"


def test_run_task_next_redirects_back_to_overview(app_factory, session_factory, monkeypatch):
    with session_factory() as db:
        group = Category(name="家居")
        db.add(group)
        db.commit()
        db.refresh(group)

        task = MonitorTask(
            name="Store D",
            url="https://example.com/d",
            selector="div.item",
            check_interval_hours=24,
            recipients="d@example.com",
            category_id=group.id,
            is_active=True,
        )
        db.add(task)
        db.commit()
        db.refresh(task)

    monkeypatch.setattr("routes.tasks.queue_monitor_batch", lambda task_ids, digest_scope: len(task_ids))

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(f"/tasks/{task.id}/run?next=/tasks", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/tasks?run_started=1"


def test_overview_issue_lists_hide_running_and_waiting_tasks(app_factory, session_factory, monkeypatch):
    with session_factory() as db:
        group = Category(name="异常分组")
        db.add(group)
        db.commit()
        db.refresh(group)

        waiting_abnormal = MonitorTask(
            name="Waiting abnormal",
            url="https://example.com/waiting-abnormal",
            selector="div.item",
            check_interval_hours=24,
            recipients="waiting@example.com",
            category_id=group.id,
            is_active=True,
            health_state="warning",
            consecutive_failure_count=1,
            last_failure_reason="等待中的异常任务",
        )
        idle_abnormal = MonitorTask(
            name="Idle abnormal",
            url="https://example.com/idle-abnormal",
            selector="div.item",
            check_interval_hours=24,
            recipients="idle@example.com",
            category_id=group.id,
            is_active=True,
            health_state="warning",
            consecutive_failure_count=2,
            last_failure_reason="待处理异常任务",
        )
        running_failed = MonitorTask(
            name="Running failed",
            url="https://example.com/running-failed",
            selector="div.item",
            check_interval_hours=24,
            recipients="running@example.com",
            category_id=group.id,
            is_active=True,
            health_state="failed",
            consecutive_failure_count=5,
            last_failure_reason="运行中的失败任务",
        )
        idle_failed = MonitorTask(
            name="Idle failed",
            url="https://example.com/idle-failed",
            selector="div.item",
            check_interval_hours=24,
            recipients="failed@example.com",
            category_id=group.id,
            is_active=True,
            health_state="failed",
            consecutive_failure_count=6,
            last_failure_reason="待处理失败任务",
        )
        db.add_all([waiting_abnormal, idle_abnormal, running_failed, idle_failed])
        db.commit()
        db.refresh(waiting_abnormal)
        db.refresh(running_failed)

    monkeypatch.setattr("routes.groups.get_queue_snapshot", lambda: (running_failed.id, [waiting_abnormal.id]))

    app = app_factory(groups_router)
    client = TestClient(app)

    response = client.get("/tasks")

    assert response.status_code == 200
    assert "Idle abnormal" in response.text
    assert "Idle failed" in response.text
    assert "Waiting abnormal" not in response.text
    assert "Running failed" not in response.text


def test_overview_issue_lists_hide_inactive_tasks_and_render_manual_review_actions(app_factory, session_factory, monkeypatch):
    with session_factory() as db:
        group = Category(name="人工确认分组")
        db.add(group)
        db.commit()
        db.refresh(group)

        active_manual = MonitorTask(
            name="Manual Active",
            url="https://example.com/manual-active",
            selector="div.item",
            check_interval_hours=24,
            recipients="active@example.com",
            category_id=group.id,
            is_active=True,
            manual_review_pending=True,
            manual_review_kind="failure",
            health_state="warning",
            last_failure_reason="captcha",
        )
        inactive_manual = MonitorTask(
            name="Manual Inactive",
            url="https://example.com/manual-inactive",
            selector="div.item",
            check_interval_hours=24,
            recipients="inactive@example.com",
            category_id=group.id,
            is_active=False,
            manual_review_pending=True,
            manual_review_kind="failure",
            health_state="warning",
            last_failure_reason="seasonal pause",
        )
        failed_task = MonitorTask(
            name="Failed Store",
            url="https://example.com/failed",
            selector="div.item",
            check_interval_hours=24,
            recipients="failed@example.com",
            category_id=group.id,
            is_active=True,
            health_state="failed",
            consecutive_failure_count=2,
            last_failure_reason="timeout",
        )
        db.add_all([active_manual, inactive_manual, failed_task])
        db.commit()
        db.refresh(active_manual)
        db.refresh(failed_task)
        group_id = group.id

    monkeypatch.setattr("routes.groups.get_queue_snapshot", lambda: (None, []))

    app = app_factory(groups_router)
    client = TestClient(app)

    response = client.get("/tasks")

    assert response.status_code == 200
    assert "Manual Active" in response.text
    assert "Manual Inactive" not in response.text
    assert f'action="/tasks/{active_manual.id}/accept-manual-review"' in response.text
    assert f'action="/tasks/{active_manual.id}/ignore-manual-review"' in response.text
    assert f'action="/tasks/{active_manual.id}/retry-manual-review"' in response.text
    assert f'action="/tasks/{failed_task.id}/reset-health"' in response.text
    assert (
        f'href="/tasks/group/{group_id}?search=Manual+Active&amp;highlight={active_manual.id}"'
        in response.text
    )
    assert 'name="next" value="/tasks"' in response.text


def test_import_legacy_to_pending_handles_missing_optional_columns(session_factory, monkeypatch):
    monkeypatch.setattr("routes.groups.validate_monitor_target_url", lambda value: value)
    monkeypatch.setattr("routes.groups.normalize_recipients", lambda value: value)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/legacy.db"
        src = sqlite3.connect(db_path)
        src.row_factory = sqlite3.Row
        src.execute(
            "CREATE TABLE monitor_tasks (name TEXT, url TEXT, check_interval_hours INTEGER, recipients TEXT)"
        )
        src.execute(
            "INSERT INTO monitor_tasks (name, url, check_interval_hours, recipients) VALUES (?, ?, ?, ?)",
            ("Legacy Store", "https://example.com/legacy", 12, "legacy@example.com"),
        )
        src.commit()

        with session_factory() as db:
            added, skipped = _import_legacy_to_pending(src, db)
            db.commit()
            pending = db.query(PendingImport).first()

        src.close()

    assert added == 1
    assert skipped == 0
    assert pending is not None
    assert pending.name == "Legacy Store"
    assert pending.selector == "div[data-component-type='s-search-result']"
    assert pending.is_active is True


def test_accept_integrity_review_updates_task_and_redirects(app_factory, session_factory, monkeypatch, tmp_path):
    state_path = tmp_path / "integrity-review.json"
    monkeypatch.setattr(scraper, "_integrity_review_state_path", lambda _task_id: state_path)
    monkeypatch.setattr("routes.tasks.remove_scheduled_task", lambda _task_id: None)
    monkeypatch.setattr("routes.tasks.schedule_task", lambda _task: None)
    resolved = []
    monkeypatch.setattr(
        "routes.tasks.resolve_manual_review_report",
        lambda scope_key, task_id, **kwargs: resolved.append((scope_key, task_id, kwargs)),
    )

    with session_factory() as db:
        group = Category(name="低数量店铺")
        db.add(group)
        db.commit()
        db.refresh(group)

        task = MonitorTask(
            name="Sparse Store",
            url="https://example.com/sparse",
            selector="div.item",
            check_interval_hours=24,
            recipients="a@example.com",
            category_id=group.id,
            is_active=True,
            health_state="warning",
            manual_review_pending=True,
            manual_review_kind="integrity",
            manual_review_scope_key="group:integrity:run:test",
            manual_review_scope_label="低数量店铺",
            manual_review_scope_kind="group",
            consecutive_failure_count=1,
            last_failure_reason="Low-count integrity review pending",
            integrity_reference_count=5,
        )
        db.add(task)
        db.commit()
        db.refresh(task)

        payload = {
            "captured_at": "2026-04-03T10:00:00+00:00",
            "current_count": 2,
            "reference_count": 5,
            "integrity_ratio": 0.75,
            "scrape_ratio": 0.40,
            "allowed_shortfall": 2,
            "min_required_count": 3,
            "ratio_required_count": 4,
            "count_required_floor": 3,
            "products": [
                {"name": "Product A", "link": "https://example.com/a", "asin": ""},
                {"name": "Product B", "link": "https://example.com/b", "asin": ""},
            ],
        }
        state_path.write_text(json.dumps(payload), encoding="utf-8")
        group_id = group.id
        task_id = task.id

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(f"/tasks/{task_id}/accept-manual-review", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == (
        f"/tasks/group/{group_id}?normalized=1&normalized_count=2&normalized_removed=0"
    )

    with session_factory() as db:
        task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
        assert task is not None
        assert task.integrity_reference_count == 2
        assert task.health_state == "healthy"
        assert task.manual_review_pending is False
        assert task.last_failure_reason is None
    assert not state_path.exists()
    assert resolved[0][0:2] == ("group:integrity:run:test", task_id)


def test_accept_failure_manual_review_clears_state_and_resumes_schedule(app_factory, session_factory, monkeypatch):
    monkeypatch.setattr("routes.tasks.remove_scheduled_task", lambda _task_id: None)
    scheduled = []
    monkeypatch.setattr("routes.tasks.schedule_task", lambda task: scheduled.append(task.id))
    resolved = []
    monkeypatch.setattr(
        "routes.tasks.resolve_manual_review_report",
        lambda scope_key, task_id, **kwargs: resolved.append((scope_key, task_id, kwargs)),
    )

    with session_factory() as db:
        group = Category(name="失败店铺")
        db.add(group)
        db.commit()
        db.refresh(group)

        task = MonitorTask(
            name="Broken Store",
            url="https://example.com/broken",
            selector="div.item",
            check_interval_hours=24,
            recipients="ops@example.com",
            category_id=group.id,
            is_active=True,
            health_state="failed",
            manual_review_pending=True,
            manual_review_kind="failure",
            manual_review_scope_key="group:failure:run:test",
            manual_review_scope_label="失败店铺",
            manual_review_scope_kind="group",
            consecutive_failure_count=3,
            last_failure_reason="captcha",
        )
        db.add(task)
        db.commit()
        db.refresh(task)
        task_id = task.id
        group_id = group.id

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(f"/tasks/{task_id}/accept-manual-review", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == f"/tasks/group/{group_id}?review_cleared=1"
    assert scheduled == [task_id]
    assert resolved[0][0:2] == ("group:failure:run:test", task_id)

    with session_factory() as db:
        task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
        assert task is not None
        assert task.health_state == "healthy"
        assert task.manual_review_pending is False
        assert task.last_failure_reason is None


def test_accept_failure_manual_review_respects_next_redirect(app_factory, session_factory, monkeypatch):
    monkeypatch.setattr("routes.tasks.remove_scheduled_task", lambda _task_id: None)
    monkeypatch.setattr("routes.tasks.schedule_task", lambda _task: None)
    monkeypatch.setattr("routes.tasks.resolve_manual_review_report", lambda *args, **kwargs: None)

    with session_factory() as db:
        group = Category(name="返回总览")
        db.add(group)
        db.commit()
        db.refresh(group)

        task = MonitorTask(
            name="Overview Accept",
            url="https://example.com/overview-accept",
            selector="div.item",
            check_interval_hours=24,
            recipients="ops@example.com",
            category_id=group.id,
            is_active=True,
            health_state="failed",
            manual_review_pending=True,
            manual_review_kind="failure",
            consecutive_failure_count=2,
            last_failure_reason="captcha",
        )
        db.add(task)
        db.commit()
        db.refresh(task)

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(
        f"/tasks/{task.id}/accept-manual-review",
        data={"next": "/tasks?search=overview"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/tasks?search=overview&review_cleared=1"


def test_ignore_integrity_review_preserves_previous_baseline(app_factory, session_factory, monkeypatch, tmp_path):
    state_path = tmp_path / "integrity-review.json"
    monkeypatch.setattr(scraper, "_integrity_review_state_path", lambda _task_id: state_path)
    monkeypatch.setattr("routes.tasks.remove_scheduled_task", lambda _task_id: None)
    scheduled = []
    monkeypatch.setattr("routes.tasks.schedule_task", lambda task: scheduled.append(task.id))
    resolved = []
    monkeypatch.setattr(
        "routes.tasks.resolve_manual_review_report",
        lambda scope_key, task_id, **kwargs: resolved.append((scope_key, task_id, kwargs)),
    )

    with session_factory() as db:
        group = Category(name="忽略低数量")
        db.add(group)
        db.commit()
        db.refresh(group)

        task = MonitorTask(
            name="Ignore Sparse",
            url="https://example.com/ignore-sparse",
            selector="div.item",
            check_interval_hours=24,
            recipients="ops@example.com",
            category_id=group.id,
            is_active=True,
            health_state="warning",
            manual_review_pending=True,
            manual_review_kind="integrity",
            manual_review_scope_key="group:integrity:ignore:test",
            manual_review_scope_label="忽略低数量",
            manual_review_scope_kind="group",
            consecutive_failure_count=1,
            last_failure_reason="Low-count integrity review pending",
            integrity_reference_count=5,
            peak_product_count=5,
        )
        db.add(task)
        db.commit()
        db.refresh(task)

        state_path.write_text(
            json.dumps({
                "current_count": 2,
                "reference_count": 5,
                "products": [
                    {"name": "Product A", "link": "https://example.com/a", "asin": ""},
                    {"name": "Product B", "link": "https://example.com/b", "asin": ""},
                ],
            }),
            encoding="utf-8",
        )
        group_id = group.id
        task_id = task.id

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(f"/tasks/{task_id}/ignore-manual-review", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == f"/tasks/group/{group_id}?review_ignored=1"
    assert scheduled == [task_id]
    assert resolved[0][0:2] == ("group:integrity:ignore:test", task_id)
    assert resolved[0][2]["resolution_message"] == "人工处理：已忽略本次结果，保留上次运行记忆"
    assert not state_path.exists()

    with session_factory() as db:
        task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
        assert task is not None
        assert task.integrity_reference_count == 5
        assert task.peak_product_count == 5
        assert task.health_state == "healthy"
        assert task.manual_review_pending is False
        assert task.manual_review_kind is None
        assert task.last_failure_reason is None


def test_retry_manual_review_queues_task_with_saved_scope(app_factory, session_factory, monkeypatch):
    queued_calls = []
    monkeypatch.setattr(
        "routes.tasks.queue_monitor_batch",
        lambda task_ids, digest_scope: queued_calls.append((task_ids, digest_scope)) or len(task_ids),
    )

    with session_factory() as db:
        group = Category(name="数码")
        db.add(group)
        db.commit()
        db.refresh(group)

        task = MonitorTask(
            name="Retry Store",
            url="https://example.com/retry",
            selector="div.item",
            check_interval_hours=24,
            recipients="ops@example.com",
            category_id=group.id,
            is_active=True,
            manual_review_pending=True,
            manual_review_kind="failure",
            manual_review_scope_key="group:99",
            manual_review_scope_label="历史分组",
            manual_review_scope_kind="group",
            last_failure_reason="timeout",
        )
        db.add(task)
        db.commit()
        db.refresh(task)
        task_id = task.id
        group_id = group.id

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(f"/tasks/{task_id}/retry-manual-review", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == f"/tasks/group/{group_id}?run_started=1"
    assert queued_calls[0][0] == [task_id]
    assert queued_calls[0][1] == {
        "key": "group:99",
        "label": "历史分组",
        "kind": "group",
    }

    with session_factory() as db:
        task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
        assert task is not None
        assert task.manual_review_pending is True
        assert task.manual_review_kind == "failure"
        assert task.last_failure_reason == "timeout"


def test_retry_manual_review_respects_next_redirect(app_factory, session_factory, monkeypatch):
    queued_calls = []
    monkeypatch.setattr(
        "routes.tasks.queue_monitor_batch",
        lambda task_ids, digest_scope: queued_calls.append((task_ids, digest_scope)) or len(task_ids),
    )

    with session_factory() as db:
        group = Category(name="重试总览")
        db.add(group)
        db.commit()
        db.refresh(group)

        task = MonitorTask(
            name="Overview Retry",
            url="https://example.com/overview-retry",
            selector="div.item",
            check_interval_hours=24,
            recipients="ops@example.com",
            category_id=group.id,
            is_active=True,
            manual_review_pending=True,
            manual_review_kind="failure",
            manual_review_scope_key="group:1",
            manual_review_scope_label="旧分组",
            manual_review_scope_kind="group",
            last_failure_reason="timeout",
        )
        db.add(task)
        db.commit()
        db.refresh(task)

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(
        f"/tasks/{task.id}/retry-manual-review",
        data={"next": "/tasks?search=retry"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/tasks?search=retry&run_started=1"
    assert queued_calls[0][0] == [task.id]
    assert queued_calls[0][1] == {
        "key": "group:1",
        "label": "旧分组",
        "kind": "group",
    }


def test_toggle_task_closing_clears_manual_review_and_keeps_current_page(app_factory, session_factory, monkeypatch):
    removed = []
    resolved = []
    monkeypatch.setattr("routes.tasks.remove_scheduled_task", lambda task_id: removed.append(task_id))
    monkeypatch.setattr(
        "routes.tasks.resolve_manual_review_report",
        lambda scope_key, task_id, **kwargs: resolved.append((scope_key, task_id, kwargs)),
    )

    with session_factory() as db:
        group = Category(name="季节品")
        db.add(group)
        db.commit()
        db.refresh(group)

        task = MonitorTask(
            name="Seasonal Store",
            url="https://example.com/seasonal",
            selector="div.item",
            check_interval_hours=24,
            recipients="ops@example.com",
            category_id=group.id,
            is_active=True,
            health_state="failed",
            consecutive_empty_count=2,
            consecutive_failure_count=3,
            manual_review_pending=True,
            manual_review_kind="failure",
            last_failure_reason="captcha",
        )
        db.add(task)
        db.commit()
        db.refresh(task)
        group_id = group.id
        task_id = task.id

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(
        f"/tasks/{task_id}/toggle",
        data={"next": f"/tasks/group/{group_id}?page=3&search=seasonal"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == f"/tasks/group/{group_id}?page=3&search=seasonal"
    assert removed == [task_id]
    assert resolved and resolved[0][1] == task_id

    with session_factory() as db:
        task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
        assert task is not None
        assert task.is_active is False
        assert task.health_state == "healthy"
        assert task.manual_review_pending is False
        assert task.consecutive_empty_count == 0
        assert task.consecutive_failure_count == 0
        assert task.last_failure_reason is None
        assert task.last_failure_at is None


def test_run_group_skips_manual_review_pending_tasks(app_factory, session_factory, monkeypatch):
    with session_factory() as db:
        group = Category(name="家清")
        db.add(group)
        db.commit()
        db.refresh(group)

        active_task = MonitorTask(
            name="Healthy Store",
            url="https://example.com/healthy",
            selector="div.item",
            check_interval_hours=24,
            recipients="a@example.com",
            category_id=group.id,
            is_active=True,
            manual_review_pending=False,
        )
        blocked_task = MonitorTask(
            name="Pending Store",
            url="https://example.com/pending",
            selector="div.item",
            check_interval_hours=24,
            recipients="b@example.com",
            category_id=group.id,
            is_active=True,
            manual_review_pending=True,
        )
        db.add_all([active_task, blocked_task])
        db.commit()
        db.refresh(group)
        active_task_id = active_task.id

    queued = []
    monkeypatch.setattr(
        "routes.tasks.queue_monitor_batch",
        lambda task_ids, digest_scope: queued.append(task_ids) or len(task_ids),
    )

    app = app_factory(tasks_router)
    client = TestClient(app)

    response = client.post(f"/tasks/group/{group.id}/run", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == f"/tasks/group/{group.id}?run_group=1&queued=1"
    assert queued == [[active_task_id]]


def test_group_detail_and_run_follow_persisted_task_order(app_factory, session_factory, monkeypatch):
    with session_factory() as db:
        group = Category(name="排序测试")
        db.add(group)
        db.commit()
        db.refresh(group)
        tasks = [
            MonitorTask(
                name=name,
                url=f"https://example.com/{name.lower()}",
                selector="div.item",
                check_interval_hours=24,
                recipients="ops@example.com",
                category_id=group.id,
                sort_order=position,
                is_active=True,
            )
            for name, position in (("Store A", 20), ("Store B", 0), ("Store C", 10))
        ]
        db.add_all(tasks)
        db.commit()
        group_id = group.id
        expected_ids = [tasks[1].id, tasks[2].id, tasks[0].id]

    queued = []
    monkeypatch.setattr("routes.tasks.queue_monitor_batch", lambda ids, _scope: queued.append(ids) or len(ids))
    app = app_factory(tasks_router)
    client = TestClient(app)

    page = client.get(f"/tasks/group/{group_id}")
    assert page.status_code == 200
    assert page.text.index("Store B") < page.text.index("Store C") < page.text.index("Store A")
    assert 'id="orderM"' in page.text

    response = client.post(f"/tasks/group/{group_id}/run", follow_redirects=False)
    assert response.status_code == 303
    assert queued == [expected_ids]


def test_reorder_group_tasks_persists_exact_permutation(app_factory, session_factory):
    with session_factory() as db:
        group = Category(name="可拖动分组")
        db.add(group)
        db.commit()
        db.refresh(group)
        tasks = [
            MonitorTask(
                name=f"Store {index}",
                url=f"https://example.com/{index}",
                selector="div.item",
                check_interval_hours=24,
                recipients="ops@example.com",
                category_id=group.id,
                sort_order=index,
                is_active=True,
            )
            for index in range(3)
        ]
        db.add_all(tasks)
        db.commit()
        group_id = group.id
        desired = [tasks[2].id, tasks[0].id, tasks[1].id]

    app = app_factory(tasks_router)
    client = TestClient(app)
    response = client.post(f"/tasks/group/{group_id}/reorder", json={"task_ids": desired})

    assert response.status_code == 200
    assert response.json() == {"ok": True, "count": 3}
    with session_factory() as db:
        stored = (
            db.query(MonitorTask.id)
            .filter(MonitorTask.category_id == group_id)
            .order_by(MonitorTask.sort_order.asc(), MonitorTask.id.asc())
            .all()
        )
    assert [row[0] for row in stored] == desired


def test_reorder_group_tasks_rejects_partial_or_duplicate_ids(app_factory, session_factory):
    with session_factory() as db:
        group = Category(name="拒绝错误排序")
        other_group = Category(name="其他分组")
        db.add_all([group, other_group])
        db.commit()
        db.refresh(group)
        db.refresh(other_group)
        first = MonitorTask(name="First", url="https://example.com/first", category_id=group.id, sort_order=0)
        second = MonitorTask(name="Second", url="https://example.com/second", category_id=group.id, sort_order=1)
        outsider = MonitorTask(name="Outsider", url="https://example.com/out", category_id=other_group.id, sort_order=0)
        db.add_all([first, second, outsider])
        db.commit()
        group_id = group.id
        first_id, second_id, outsider_id = first.id, second.id, outsider.id

    app = app_factory(tasks_router)
    client = TestClient(app)

    duplicate = client.post(
        f"/tasks/group/{group_id}/reorder",
        json={"task_ids": [first_id, first_id]},
    )
    cross_group = client.post(
        f"/tasks/group/{group_id}/reorder",
        json={"task_ids": [first_id, outsider_id]},
    )
    partial = client.post(
        f"/tasks/group/{group_id}/reorder",
        json={"task_ids": [second_id]},
    )

    assert duplicate.status_code == 400
    assert cross_group.status_code == 400
    assert partial.status_code == 400
