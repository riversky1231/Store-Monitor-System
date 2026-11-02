from fastapi import FastAPI
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
        "report_delivery": {
            "pending_count": 0,
            "sending_count": 0,
            "failed_count": 0,
            "held_count": 0,
            "unsent_count": 0,
            "latest_error": "",
            "latest_scope": "",
            "latest_attempt_count": 0,
            "latest_sent_at": None,
            "latest_held_scope": "",
            "page": 1,
            "page_size": 10,
            "total_reports": 0,
            "total_pages": 1,
            "reports": [],
        },
    }


def test_report_delivery_retry_returns_safe_counts_and_status(app_factory, monkeypatch):
    async def _run_inline(func):
        return func()

    monkeypatch.setattr("routes.api.asyncio.to_thread", _run_inline)
    monkeypatch.setattr(
        "routes.api.retry_report_delivery_now",
        lambda: {"attempted": 1, "sent": 1, "failed": 0},
    )

    app = app_factory(api_router)
    client = TestClient(app)

    response = client.post("/api/report-delivery/retry")

    assert response.status_code == 200
    payload = response.json()
    assert payload["attempted"] == 1
    assert payload["sent"] == 1
    assert payload["failed"] == 0
    assert payload["busy"] is False
    assert payload["report_delivery"]["held_count"] == 0
    assert "recipients" not in payload
    assert "payload_json" not in payload


def test_report_delivery_retry_is_post_only(app_factory):
    app = app_factory(api_router)
    client = TestClient(app)

    response = client.get("/api/report-delivery/retry")

    assert response.status_code == 405


def test_single_report_retry_returns_selected_result(app_factory, monkeypatch):
    async def _run_inline(func, *args):
        return func(*args)

    monkeypatch.setattr("routes.api.asyncio.to_thread", _run_inline)
    monkeypatch.setattr(
        "routes.api.retry_report_delivery_by_id",
        lambda report_id: {
            "attempted": 1,
            "sent": int(report_id == 7),
            "failed": 0,
            "busy": 0,
            "unavailable": 0,
        },
    )
    app = app_factory(api_router)
    client = TestClient(app)

    response = client.post("/api/report-delivery/7/retry")

    assert response.status_code == 200
    assert response.json()["sent"] == 1
    assert response.json()["unavailable"] is False


def test_single_report_retry_rejects_invalid_id(app_factory):
    app = app_factory(api_router)
    client = TestClient(app)

    response = client.post("/api/report-delivery/0/retry")

    assert response.status_code == 422


def test_report_delivery_retry_requires_admin_auth(monkeypatch):
    monkeypatch.delenv("MONITOR_WEB_DISABLE_AUTH", raising=False)
    monkeypatch.setenv("MONITOR_WEB_REQUIRE_HTTPS", "0")
    monkeypatch.setenv("MONITOR_WEB_USERNAME", "admin")
    monkeypatch.setenv("MONITOR_WEB_PASSWORD", "retry-secret")
    app = FastAPI()
    app.include_router(api_router)
    client = TestClient(app)

    response = client.post("/api/report-delivery/retry")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == 'Basic realm="Store Monitor"'


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
            "network_healthy": False,
            "alert_active": True,
            "alert_event_id": 7,
            "last_issue_at": "2026-03-22T10:00:00+00:00",
            "last_check": "2026-03-22T10:05:00+00:00",
            "popup_cooldown_seconds": 120,
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
    assert payload["state"] == "issue"
    assert payload["event_id"] == 7


def test_network_check_reports_success(app_factory, monkeypatch):
    async def _run_inline(func):
        return func()

    monkeypatch.setattr("routes.api.asyncio.to_thread", _run_inline)
    monkeypatch.setattr(
        "routes.api.run_manual_network_check",
        lambda: {
            "success": True,
            "message": "✅ 网络状态良好！平均响应 123ms，适合抓取。",
            "response_time_ms": 123,
            "details": [
                {"name": "Amazon 主站", "status": "✅ 正常", "time_ms": 123, "note": ""},
                {"name": "Amazon robots.txt", "status": "✅ 正常", "time_ms": 123, "note": ""},
            ],
        },
    )

    app = app_factory(api_router)
    client = TestClient(app)

    response = client.get("/api/network-check")
    payload = response.json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["response_time_ms"] == 123
    assert "网络状态良好" in payload["message"]
    assert len(payload["details"]) == 2


def test_traffic_stats_returns_snapshot(app_factory, monkeypatch):
    monkeypatch.setattr(
        "routes.api.get_traffic_snapshot",
        lambda: {
            "started_at": "2026-04-03T10:00:00+00:00",
            "started_since_label": "12m 3s",
            "totals": {
                "request_count": 88,
                "upload_bytes": 1024,
                "upload_human": "1.0 KB",
                "download_bytes": 5242880,
                "download_human": "5.0 MB",
                "completed_task_count": 4,
            },
            "active_task": None,
            "last_completed_task": {
                "task_id": 9,
                "task_name": "Task 9",
                "status": "success",
                "duration_label": "1m 2s",
                "request_count": 20,
                "download_human": "1.5 MB",
                "unknown_response_count": 0,
                "resource_breakdown": [],
            },
        },
    )

    app = app_factory(api_router)
    client = TestClient(app)

    response = client.get("/api/traffic-stats")
    payload = response.json()

    assert response.status_code == 200
    assert payload["totals"]["download_human"] == "5.0 MB"
    assert payload["totals"]["request_count"] == 88
    assert payload["last_completed_task"]["task_name"] == "Task 9"
