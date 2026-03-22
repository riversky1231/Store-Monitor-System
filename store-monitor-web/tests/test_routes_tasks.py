from fastapi.testclient import TestClient

from models import Category, MonitorTask
from routes.groups import router as groups_router
from routes.tasks import router as tasks_router


def test_groups_page_escapes_error_query_param(app_factory):
    app = app_factory(groups_router)
    client = TestClient(app)

    response = client.get("/tasks?error=%3Cscript%3Ealert(1)%3C%2Fscript%3E")

    assert response.status_code == 200
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in response.text
    assert '<div class="alert alert-err"><script>alert(1)</script></div>' not in response.text


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
