from types import SimpleNamespace

from fastapi.testclient import TestClient

from models import MonitorTask
from routes.api import router as api_router


def test_queue_status_returns_named_items(app_factory, session_factory, monkeypatch):
    with session_factory() as db:
        task1 = MonitorTask(
            name="Task A",
            url="https://example.com/a",
            selector="div.item",
            check_interval_hours=24,
            recipients="a@example.com",
            is_active=True,
        )
        task2 = MonitorTask(
            name="Task B",
            url="https://example.com/b",
            selector="div.item",
            check_interval_hours=24,
            recipients="b@example.com",
            is_active=True,
        )
        db.add_all([task1, task2])
        db.commit()
        db.refresh(task1)
        db.refresh(task2)

    monkeypatch.setattr("routes.api.get_queue_snapshot", lambda: (task1.id, [task2.id]))

    app = app_factory(api_router)
    client = TestClient(app)

    response = client.get("/api/queue-status")

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {"id": task1.id, "name": "Task A", "status": "running"},
            {"id": task2.id, "name": "Task B", "status": "waiting"},
        ],
        "total": 2,
    }


def test_network_alert_status_returns_task_preview(app_factory, session_factory, monkeypatch):
    with session_factory() as db:
        tasks = [
            MonitorTask(
                name=f"Task {idx}",
                url=f"https://example.com/{idx}",
                selector="div.item",
                check_interval_hours=24,
                recipients=f"{idx}@example.com",
                is_active=True,
            )
            for idx in range(1, 5)
        ]
        db.add_all(tasks)
        db.commit()
        for task in tasks:
            db.refresh(task)

    monkeypatch.setattr(
        "routes.api.get_network_retry_status",
        lambda: {
            "pending_tasks": [task.id for task in tasks],
            "pending_count": 4,
            "alert_event_id": 7,
            "last_issue_at": "2026-03-22T10:00:00+00:00",
            "last_check": "2026-03-22T10:05:00+00:00",
            "cooldown_seconds": 120,
        },
    )

    app = app_factory(api_router)
    client = TestClient(app)

    response = client.get("/api/network-alert-status")
    payload = response.json()

    assert response.status_code == 200
    assert payload["pending_count"] == 4
    assert payload["pending_preview"] == ["Task 1", "Task 2", "Task 3"]
    assert payload["network_healthy"] is False
    assert payload["event_id"] == 7


def test_network_check_reports_success(app_factory, monkeypatch):
    async def _run_inline(func):
        return func()

    probe = SimpleNamespace(
        error_kind=None,
        error_message="",
        status_code=200,
        final_url="https://www.amazon.com",
        body_text="ok",
        elapsed_ms=123,
    )

    monkeypatch.setattr("routes.api.asyncio.to_thread", _run_inline)
    monkeypatch.setattr("routes.api.probe_http_text", lambda *args, **kwargs: probe)
    monkeypatch.setattr("routes.api.response_looks_blocked", lambda *args, **kwargs: False)

    app = app_factory(api_router)
    client = TestClient(app)

    response = client.get("/api/network-check")
    payload = response.json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["response_time_ms"] == 123
    assert "网络状态良好" in payload["message"]
    assert len(payload["details"]) == 2
