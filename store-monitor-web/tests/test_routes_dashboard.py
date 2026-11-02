from fastapi.testclient import TestClient

from models import MonitorTask, ProductItem
from routes.dashboard import router as dashboard_router


def _traffic_snapshot():
    return {
        "started_at": "2026-04-03T10:00:00+00:00",
        "started_since_label": "8m 10s",
        "totals": {
            "request_count": 42,
            "upload_bytes": 512,
            "upload_human": "512 B",
            "download_bytes": 1048576,
            "download_human": "1.0 MB",
            "completed_task_count": 2,
        },
        "active_task": {
            "task_id": 1,
            "task_name": "Running Task",
            "status": "running",
            "duration_label": "33s",
            "request_count": 8,
            "download_human": "256.0 KB",
            "unknown_response_count": 0,
            "resource_breakdown": [
                {"type": "document", "download_human": "120.0 KB", "request_count": 1},
            ],
        },
        "last_completed_task": {
            "task_id": 2,
            "task_name": "Last Task",
            "status": "success",
            "duration_label": "1m 14s",
            "request_count": 20,
            "download_human": "768.0 KB",
            "unknown_response_count": 1,
            "resource_breakdown": [
                {"type": "xhr", "download_human": "300.0 KB", "request_count": 4},
            ],
        },
    }


def test_dashboard_renders_traffic_panel(app_factory, monkeypatch):
    monkeypatch.setattr(
        "routes.dashboard.get_traffic_snapshot",
        _traffic_snapshot,
    )

    app = app_factory(dashboard_router)
    client = TestClient(app)

    response = client.get("/")
    html = response.text

    assert response.status_code == 200
    assert "抓取流量" in html
    assert "1.0 MB" in html
    assert "Running Task" in html
    assert "Last Task" in html


def test_dashboard_recent_products_falls_back_to_latest_inserted_when_discovery_time_missing(
    app_factory,
    session_factory,
    monkeypatch,
):
    monkeypatch.setattr("routes.dashboard.get_traffic_snapshot", _traffic_snapshot)

    with session_factory() as db:
        task = MonitorTask(
            name="Fallback Task",
            url="https://example.com/fallback",
            selector="div.item",
            check_interval_hours=24,
            recipients="ops@example.com",
            is_active=True,
        )
        db.add(task)
        db.commit()
        db.refresh(task)

        db.add_all([
            ProductItem(
                task_id=task.id,
                product_link="https://example.com/products/old",
                name="Old Product",
                discovered_at=None,
            ),
            ProductItem(
                task_id=task.id,
                product_link="https://example.com/products/new",
                name="New Product",
                discovered_at=None,
            ),
        ])
        db.commit()

    app = app_factory(dashboard_router)
    client = TestClient(app)

    response = client.get("/")
    html = response.text

    assert response.status_code == 200
    assert html.index("New Product") < html.index("Old Product")


def test_dashboard_renders_recent_product_when_task_is_missing(
    app_factory,
    session_factory,
    monkeypatch,
):
    monkeypatch.setattr("routes.dashboard.get_traffic_snapshot", _traffic_snapshot)

    with session_factory() as db:
        db.add(
            ProductItem(
                task_id=999,
                product_link="https://example.com/products/orphan",
                name="Orphan Product",
                discovered_at=None,
            )
        )
        db.commit()

    app = app_factory(dashboard_router)
    client = TestClient(app)

    response = client.get("/")
    html = response.text

    assert response.status_code == 200
    assert "Orphan Product" in html
    assert "已删除任务" in html
