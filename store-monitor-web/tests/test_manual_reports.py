import datetime
from email.mime.text import MIMEText
import json
import ssl
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

import scheduler
import scheduler_health
import scheduler_notifications
from models import Category, MonitorTask, ProductItem, ReportOutbox, SystemConfig
from routes.groups import router as groups_router


def _task(name: str, group_id: int | None = None) -> MonitorTask:
    return MonitorTask(
        name=name,
        url=f"https://example.com/{name.lower().replace(' ', '-')}",
        selector="div.item",
        check_interval_hours=24,
        recipients=f"{name.lower().replace(' ', '-')}@example.com",
        category_id=group_id,
        is_active=True,
    )


def _report_entry(task: MonitorTask, scope_key: str, *, status: str, products=None) -> dict:
    return {
        "task_id": task.id,
        "task_name": task.name,
        "group_id": task.category_id,
        "group_name": task.group.name if task.group else "未分组",
        "scope_key": scope_key,
        "scope_label": task.group.name if task.group else "未分组",
        "scope_kind": "group",
        "new_products": [],
        "removed_products": [],
        "current_products": list(products or []),
        "is_baseline": False,
        "baseline_count": 0,
        "status": status,
        "status_message": task.last_failure_reason or "",
        "started_at": "",
        "task_url": task.url,
        "recipients": [task.recipients],
    }


def test_run_all_creates_one_manual_report_batch_per_group(app_factory, session_factory, monkeypatch):
    with session_factory() as db:
        group_a = Category(name="分组 A")
        group_b = Category(name="分组 B")
        db.add_all([group_a, group_b])
        db.commit()
        db.refresh(group_a)
        db.refresh(group_b)
        task_a = _task("Store A", group_a.id)
        task_b = _task("Store B", group_b.id)
        db.add_all([task_a, task_b])
        db.commit()
        task_ids = {task_a.id, task_b.id}

    batches = []

    def _fake_batch(ids, scope):
        batches.append((list(ids), dict(scope)))
        return len(ids)

    monkeypatch.setattr("routes.groups.queue_monitor_batch", _fake_batch)
    client = TestClient(app_factory(groups_router))

    response = client.post("/tasks/run-all", follow_redirects=False)

    assert response.status_code == 303
    query = parse_qs(urlparse(response.headers["location"]).query)
    assert query == {"run_all": ["1"], "queued": ["2"], "reports": ["2"]}
    assert len(batches) == 2
    assert {ids[0] for ids, _scope in batches} == task_ids
    assert {scope["label"] for _ids, scope in batches} == {"分组 A", "分组 B"}
    assert all(scope["kind"] == "group" for _ids, scope in batches)
    assert len({scope["key"] for _ids, scope in batches}) == 2


def test_success_without_changes_still_queues_complete_manual_report():
    task = _task("Unchanged Store")
    task.consecutive_empty_count = 0
    db = MagicMock()
    queue_report = MagicMock()
    current = [
        {"name": "Product A", "link": "https://example.com/products/a"},
        {"name": "Product B", "link": "https://example.com/products/b"},
    ]

    scheduler_health.handle_successful_scrape(
        db,
        task,
        current,
        [],
        [],
        False,
        empty_alert_threshold=3,
        send_recovery_notification=MagicMock(),
        queue_digest_entry=queue_report,
        is_queue_worker_thread=lambda: True,
        send_email=MagicMock(),
    )

    queue_report.assert_called_once()
    assert queue_report.call_args.kwargs["current_products"] == current
    assert queue_report.call_args.kwargs["status"] == "success"
    assert task.next_run_at is None


def test_group_report_renders_all_current_product_links(session_factory):
    with session_factory() as db:
        group = Category(name="链接分组")
        db.add(group)
        db.commit()
        db.refresh(group)
        task = _task("Link Store", group.id)
        db.add(task)
        db.add(SystemConfig(sender_email="sender@example.com", sender_password="enc::dummy"))
        db.commit()
        db.refresh(task)

        products = [
            {"name": f"Product {index}", "link": f"https://example.com/products/{index}"}
            for index in range(75)
        ]
        entries = [{
            "task_id": task.id,
            "task_name": task.name,
            "group_id": group.id,
            "group_name": group.name,
            "scope_key": f"group:{group.id}:run:test",
            "scope_label": group.name,
            "scope_kind": "group",
            "new_products": [],
            "removed_products": [],
            "current_products": products,
            "is_baseline": False,
            "baseline_count": 0,
            "status": "success",
            "status_message": "",
            "started_at": "",
        }]
        sent: dict[str, MIMEText] = {}

        def _capture(_config, _password, msg, _recipients, retry_attempts=2):
            sent["message"] = msg

        with patch("scheduler_notifications.decrypt_secret", return_value="secret"), patch(
            "scheduler_notifications.smtp_send", side_effect=_capture
        ):
            assert scheduler_notifications.send_consolidated_email(db, entries)

    message = sent["message"]
    assert message.is_multipart()
    body_part, attachment_part = message.get_payload()

    body = body_part.get_payload(decode=True).decode("utf-8")
    assert body_part.get_filename() is None
    assert "<details" not in body
    assert "https://example.com/products/0" not in body
    assert "当前全部商品 <b>75</b> 个" in body

    filename = attachment_part.get_filename()
    assert attachment_part.get_content_disposition() == "attachment"
    assert filename.startswith("链接分组_全部商品清单_") and filename.endswith(".html")
    assert filename in body

    attachment = attachment_part.get_payload(decode=True).decode("utf-8")
    document = BeautifulSoup(attachment, "html.parser")
    assert "Link Store (75)" in document.find("h3", attrs={"data-section": "store"}).get_text()
    assert len(document.select("tbody tr")) == 75
    assert "https://example.com/products/0" in attachment
    assert "https://example.com/products/74" in attachment
    assert "未显示" not in attachment


def test_group_report_keeps_new_products_in_body_and_splits_stores_in_attachment(session_factory):
    with session_factory() as db:
        group = Category(name="多店铺/分组")
        db.add(group)
        db.commit()
        db.refresh(group)
        task_a = _task("Store A", group.id)
        task_b = _task("Store B", group.id)
        db.add_all([task_a, task_b])
        db.add(SystemConfig(sender_email="sender@example.com", sender_password="enc::dummy"))
        db.commit()
        db.refresh(task_a)
        db.refresh(task_b)

        new_product = {"name": "Fresh Product", "link": "https://example.com/products/fresh"}
        old_product = {"name": "Old Product", "link": "https://example.com/products/old"}
        scope_key = f"group:{group.id}:run:split"
        entry_a = _report_entry(task_a, scope_key, status="success", products=[new_product, old_product])
        entry_a["new_products"] = [new_product]
        entry_b = _report_entry(task_b, scope_key, status="success", products=[old_product])
        sent = {}

        def _capture(_config, _password, msg, _recipients, retry_attempts=2):
            sent["message"] = msg

        with patch("scheduler_notifications.decrypt_secret", return_value="secret"), patch(
            "scheduler_notifications.smtp_send", side_effect=_capture
        ):
            assert scheduler_notifications.send_consolidated_email(db, [entry_a, entry_b])

    body_part, attachment_part = sent["message"].get_payload()
    body = body_part.get_payload(decode=True).decode("utf-8")
    assert "https://example.com/products/fresh" in body
    assert "https://example.com/products/old" not in body

    filename = attachment_part.get_filename()
    assert "/" not in filename
    assert filename.startswith("多店铺_分组_全部商品清单_")

    attachment = attachment_part.get_payload(decode=True).decode("utf-8")
    document = BeautifulSoup(attachment, "html.parser")
    store_headings = [
        heading.get_text() for heading in document.find_all("h3", attrs={"data-section": "store"})
    ]
    assert store_headings == ["Store A (2)", "Store B (1)"]
    assert len(document.select("tbody tr")) == 3


def test_report_without_current_products_has_no_attachment(session_factory):
    with session_factory() as db:
        group = Category(name="空分组")
        db.add(group)
        db.commit()
        db.refresh(group)
        task = _task("Empty Store", group.id)
        db.add(task)
        db.add(SystemConfig(sender_email="sender@example.com", sender_password="enc::dummy"))
        db.commit()
        db.refresh(task)
        entry = _report_entry(task, f"group:{group.id}:run:empty", status="success", products=[])
        sent = {}

        def _capture(_config, _password, msg, _recipients, retry_attempts=2):
            sent["message"] = msg

        with patch("scheduler_notifications.decrypt_secret", return_value="secret"), patch(
            "scheduler_notifications.smtp_send", side_effect=_capture
        ):
            assert scheduler_notifications.send_consolidated_email(db, [entry])

    assert not sent["message"].is_multipart()
    body = sent["message"].get_payload(decode=True).decode("utf-8")
    assert "本次运行未返回可展示的商品链接" in body


def test_manual_review_reminder_renders_only_action_items(session_factory):
    with session_factory() as db:
        db.add(SystemConfig(sender_email="sender@example.com", sender_password="enc::dummy"))
        db.commit()
        entries = [
            {
                "task_id": 501,
                "task_name": "Needs Review Store",
                "scope_key": "group:5:run:reminder-body",
                "scope_label": "分组 5",
                "scope_kind": "group",
                "status": "manual_review",
                "status_message": "抓取数量明显偏低",
                "recipients": ["ops@example.com"],
                # Reminder rendering must ignore report/product data even if a
                # malformed caller accidentally includes it.
                "current_products": [
                    {"name": "Must Stay Hidden", "link": "https://example.com/hidden-product"}
                ],
            }
        ]
        sent: dict[str, MIMEText] = {}

        def _capture(_config, _password, msg, recipients, retry_attempts=2):
            sent["message"] = msg
            sent["recipients"] = recipients

        with patch("scheduler_notifications.decrypt_secret", return_value="secret"), patch(
            "scheduler_notifications.smtp_send", side_effect=_capture
        ):
            assert scheduler_notifications.send_manual_review_reminder(db, entries)

    body = sent["message"].get_payload(decode=True).decode("utf-8")
    assert sent["recipients"] == ["ops@example.com"]
    assert "Needs Review Store" in body
    assert "抓取数量明显偏低" in body
    assert "Must Stay Hidden" not in body
    assert "hidden-product" not in body
    assert "完整报告已暂存" in body


def test_failed_report_entry_is_sent_instead_of_blocking_scope(session_factory):
    with session_factory() as db:
        group = Category(name="失败分组")
        db.add(group)
        db.commit()
        db.refresh(group)
        task = _task("Failed Store", group.id)
        db.add(task)
        db.add(SystemConfig(sender_email="sender@example.com", sender_password="enc::dummy"))
        db.commit()
        db.refresh(task)

        entries = [{
            "task_id": task.id,
            "task_name": task.name,
            "group_id": group.id,
            "group_name": group.name,
            "scope_key": f"group:{group.id}:run:failed",
            "scope_label": group.name,
            "scope_kind": "group",
            "new_products": [],
            "removed_products": [],
            "current_products": [],
            "is_baseline": False,
            "baseline_count": 0,
            "status": "failed",
            "status_message": "captcha detected",
            "started_at": "",
        }]
        sent = {}

        def _capture(_config, _password, msg, _recipients, retry_attempts=2):
            sent["message"] = msg

        with patch("scheduler_notifications.decrypt_secret", return_value="secret"), patch(
            "scheduler_notifications.smtp_send", side_effect=_capture
        ):
            assert scheduler_notifications.send_consolidated_email(db, entries)

    body = sent["message"].get_payload(decode=True).decode("utf-8")
    assert "运行失败" in body
    assert "captcha detected" in body


def test_durable_report_uses_snapshotted_recipient_after_task_deleted(session_factory):
    with session_factory() as db:
        db.add(SystemConfig(sender_email="sender@example.com", sender_password="enc::dummy"))
        db.commit()
        entries = [{
            "task_id": 999,
            "task_name": "Deleted Store",
            "scope_key": "task:999:run:deleted",
            "scope_label": "Deleted Store",
            "scope_kind": "task",
            "new_products": [],
            "removed_products": [],
            "current_products": [],
            "status": "failed",
            "status_message": "task removed after run",
            "recipients": ["snapshot@example.com"],
            "task_url": "https://example.com/deleted-store",
        }]
        sent = {}

        def _capture(_config, _password, msg, recipients, retry_attempts=2):
            sent["recipients"] = recipients
            sent["message"] = msg

        with patch("scheduler_notifications.decrypt_secret", return_value="secret"), patch(
            "scheduler_notifications.smtp_send", side_effect=_capture
        ):
            assert scheduler_notifications.send_consolidated_email(db, entries)

    assert sent["recipients"] == ["snapshot@example.com"]
    body = sent["message"].get_payload(decode=True).decode("utf-8")
    assert "https://example.com/deleted-store" in body


def test_active_task_is_not_scheduled_automatically():
    task = _task("Manual Store")
    task.id = 912
    with patch.object(scheduler.scheduler, "get_job", return_value=None), patch.object(
        scheduler.scheduler, "add_job"
    ) as add_job:
        scheduler.schedule_task(task)
    add_job.assert_not_called()


def test_scheduler_startup_keeps_manual_mode_and_adds_outbox_retry(monkeypatch):
    class Job:
        def __init__(self, job_id):
            self.id = job_id

    class FakeScheduler:
        def __init__(self):
            self.running = False
            self.jobs = [
                Job("task_1"),
                Job(scheduler.DAILY_STATUS_JOB_ID),
                Job(scheduler.DAILY_STATUS_WATCHDOG_JOB_ID),
                Job(scheduler.TASK_JOB_REPAIR_JOB_ID),
                Job(scheduler.NETWORK_CHECK_JOB_ID),
            ]
            self.added = []

        def get_jobs(self):
            return list(self.jobs)

        def get_job(self, job_id):
            return next((job for job in self.jobs if job.id == job_id), None)

        def remove_job(self, job_id):
            self.jobs = [job for job in self.jobs if job.id != job_id]

        def start(self):
            self.running = True

        def add_job(self, *args, **kwargs):
            self.added.append((args, kwargs))

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.started = False

        def is_alive(self):
            return self.started

        def start(self):
            self.started = True

    fake_scheduler = FakeScheduler()
    monkeypatch.setattr(scheduler, "scheduler", fake_scheduler)
    monkeypatch.setattr(scheduler, "_worker_thread", None)
    monkeypatch.setattr(scheduler.threading, "Thread", FakeThread)
    scheduler.init_scheduler()

    assert fake_scheduler.running
    assert len(fake_scheduler.added) == 1
    assert fake_scheduler.added[0][1]["id"] == scheduler.REPORT_OUTBOX_RETRY_JOB_ID
    assert fake_scheduler.added[0][1]["kwargs"]["respect_schedule"] is True
    assert [job.id for job in fake_scheduler.jobs] == [scheduler.NETWORK_CHECK_JOB_ID]


def test_manual_batch_completes_only_after_its_last_task():
    scope = {"key": "group:7:run:test", "label": "分组 7", "kind": "group"}
    scheduler._manual_batch_remaining[scope["key"]] = {101, 102}
    try:
        assert scheduler._complete_manual_batch_task(101, scope) is None
        assert scheduler._complete_manual_batch_task(102, scope) == scope["key"]
        assert scope["key"] not in scheduler._manual_batch_remaining
    finally:
        scheduler._manual_batch_remaining.pop(scope["key"], None)


def test_failed_email_delivery_is_persisted_and_can_be_retried(session_factory):
    scope_key = "group:8:run:delivery-failure"
    entry = {
        "task_id": 8,
        "task_name": "Delivery Failure Store",
        "scope_key": scope_key,
        "scope_label": "分组 8",
        "scope_kind": "group",
        "new_products": [],
        "removed_products": [],
        "current_products": [],
        "status": "failed",
        "recipients": ["reports@example.com"],
    }

    with scheduler_notifications._DIGEST_LOCK:
        scheduler_notifications._PENDING_DIGEST.clear()
        scheduler_notifications._PENDING_DIGEST.append(entry)
    try:
        with patch("scheduler_notifications.send_consolidated_email", return_value=False):
            scheduler_notifications.flush_digest(
                session_factory,
                only_scope_keys={scope_key},
            )
        with scheduler_notifications._DIGEST_LOCK:
            assert scheduler_notifications._PENDING_DIGEST == []
        with session_factory() as db:
            row = db.query(ReportOutbox).filter(ReportOutbox.scope_key == scope_key).one()
            assert row.status == "failed"
            assert row.attempt_count == 1
            assert row.last_error == "邮件发送失败，请查看日志后重试。"
            assert row.next_attempt_at is not None

        with patch("scheduler_notifications.send_consolidated_email", return_value=True):
            result = scheduler_notifications.retry_pending_reports(session_factory)

        assert result == {"attempted": 1, "sent": 1, "failed": 0}
        with session_factory() as db:
            row = db.query(ReportOutbox).filter(ReportOutbox.scope_key == scope_key).one()
            assert row.status == "sent"
            assert row.attempt_count == 2
            assert row.sent_at is not None
            assert row.next_attempt_at is None
            assert json.loads(row.payload_json) == [entry]
    finally:
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.clear()


def test_automatic_retry_waits_until_next_attempt_at(session_factory):
    now = datetime.datetime.now(datetime.timezone.utc)
    entry = {
        "task_id": 88,
        "task_name": "Delayed Retry Store",
        "scope_key": "group:88:run:delayed",
        "scope_label": "延迟重试分组",
        "scope_kind": "group",
        "new_products": [],
        "removed_products": [],
        "current_products": [],
        "status": "success",
        "recipients": ["reports@example.com"],
    }
    with session_factory() as db:
        row = ReportOutbox(
            scope_key=entry["scope_key"],
            scope_label=entry["scope_label"],
            scope_kind="group",
            payload_json=json.dumps([entry]),
            status="failed",
            attempt_count=1,
            next_attempt_at=now + datetime.timedelta(minutes=5),
        )
        db.add(row)
        db.commit()

    with patch("scheduler_notifications.send_consolidated_email", return_value=True) as send:
        result = scheduler_notifications.retry_pending_reports(
            session_factory,
            respect_schedule=True,
        )
    assert result == {"attempted": 0, "sent": 0, "failed": 0}
    send.assert_not_called()

    with session_factory() as db:
        row = db.query(ReportOutbox).filter(ReportOutbox.scope_key == entry["scope_key"]).one()
        row.next_attempt_at = now - datetime.timedelta(seconds=1)
        db.commit()

    with patch("scheduler_notifications.send_consolidated_email", return_value=True):
        result = scheduler_notifications.retry_pending_reports(
            session_factory,
            respect_schedule=True,
        )
    assert result == {"attempted": 1, "sent": 1, "failed": 0}


def test_sent_report_can_be_resent_by_id(session_factory):
    entry = {
        "task_id": 9,
        "task_name": "Resend Store",
        "scope_key": "group:9:run:resend",
        "scope_label": "重发分组",
        "scope_kind": "group",
        "new_products": [],
        "removed_products": [],
        "current_products": [],
        "status": "success",
        "recipients": ["reports@example.com"],
    }
    with session_factory() as db:
        row = ReportOutbox(
            scope_key=entry["scope_key"],
            scope_label=entry["scope_label"],
            scope_kind="group",
            payload_json=json.dumps([entry]),
            status="sent",
            attempt_count=1,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        report_id = row.id

    with patch("scheduler_notifications.send_consolidated_email", return_value=True) as send:
        result = scheduler_notifications.retry_report_by_id(session_factory, report_id)

    assert result == {"attempted": 1, "sent": 1, "failed": 0, "busy": 0, "unavailable": 0}
    send.assert_called_once()
    with session_factory() as db:
        row = db.query(ReportOutbox).filter(ReportOutbox.id == report_id).one()
        assert row.status == "sent"
        assert row.attempt_count == 2
        assert json.loads(row.payload_json) == [entry]


def test_manual_retry_sends_failed_reminder_but_keeps_full_report_held(session_factory):
    scope_key = "group:81:run:manual-resend"
    with session_factory() as db:
        group = Category(name="补发分组")
        db.add(group)
        db.commit()
        pending = _task("Pending Resend", group.id)
        pending.manual_review_pending = True
        pending.manual_review_kind = "failure"
        pending.manual_review_scope_key = scope_key
        pending.manual_review_scope_label = group.name
        pending.manual_review_scope_kind = "group"
        pending.last_failure_reason = "需要人工确认"
        db.add(pending)
        db.commit()
        db.refresh(pending)
        entry = _report_entry(pending, scope_key, status="failed")

    with scheduler_notifications._DIGEST_LOCK:
        scheduler_notifications._PENDING_DIGEST.clear()
        scheduler_notifications._PENDING_DIGEST.append(entry)
    try:
        with patch("scheduler_notifications.send_manual_review_reminder", return_value=False):
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={scope_key})

        with patch("scheduler_notifications.send_manual_review_reminder", return_value=True), patch(
            "scheduler_notifications.send_consolidated_email"
        ) as complete_send:
            result = scheduler_notifications.retry_pending_reports(
                session_factory,
                wait_for_lock=False,
            )

        assert result == {"attempted": 1, "sent": 1, "failed": 0}
        complete_send.assert_not_called()
        with session_factory() as db:
            full = db.query(ReportOutbox).filter(ReportOutbox.scope_key == scope_key).one()
            reminder = (
                db.query(ReportOutbox)
                .filter(ReportOutbox.scope_kind == "review_reminder")
                .one()
            )
            assert full.status == "held_review"
            assert reminder.status == "sent"
    finally:
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.clear()


def test_manual_retry_returns_busy_without_waiting_for_delivery_lock(session_factory):
    scheduler_notifications._OUTBOX_DELIVERY_LOCK.acquire()
    try:
        result = scheduler_notifications.retry_pending_reports(
            session_factory,
            wait_for_lock=False,
        )
    finally:
        scheduler_notifications._OUTBOX_DELIVERY_LOCK.release()

    assert result == {"attempted": 0, "sent": 0, "failed": 0, "busy": 1}


def test_durable_delivery_persists_actionable_tls_error(session_factory):
    scope_key = "group:82:run:tls-error"
    entry = {
        "task_id": 82,
        "task_name": "TLS Error Store",
        "scope_key": scope_key,
        "scope_label": "分组 82",
        "scope_kind": "group",
        "new_products": [],
        "removed_products": [],
        "current_products": [],
        "status": "failed",
        "recipients": ["reports@example.com"],
    }
    with scheduler_notifications._DIGEST_LOCK:
        scheduler_notifications._PENDING_DIGEST.clear()
        scheduler_notifications._PENDING_DIGEST.append(entry)
    try:
        with patch(
            "scheduler_notifications.send_consolidated_email",
            side_effect=ssl.SSLError("unexpected EOF"),
        ):
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={scope_key})

        with session_factory() as db:
            row = db.query(ReportOutbox).filter(ReportOutbox.scope_key == scope_key).one()
            assert row.status == "failed"
            assert row.last_error == "邮件发送失败，请查看日志后重试。"
            assert "unexpected EOF" not in row.last_error
    finally:
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.clear()


def test_durable_delivery_persists_proxy_interception_hint(session_factory):
    scope_key = "group:83:run:proxy-intercepted"
    entry = {
        "task_id": 83,
        "task_name": "Proxy Store",
        "scope_key": scope_key,
        "scope_label": "分组 83",
        "scope_kind": "group",
        "new_products": [],
        "removed_products": [],
        "current_products": [],
        "status": "failed",
        "recipients": ["reports@example.com"],
    }
    intercepted = ssl.SSLEOFError(8, "EOF occurred in violation of protocol")
    intercepted.proxy_intercepted = True
    with scheduler_notifications._DIGEST_LOCK:
        scheduler_notifications._PENDING_DIGEST.clear()
        scheduler_notifications._PENDING_DIGEST.append(entry)
    try:
        with patch(
            "scheduler_notifications.send_consolidated_email",
            side_effect=intercepted,
        ):
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={scope_key})

        with session_factory() as db:
            row = db.query(ReportOutbox).filter(ReportOutbox.scope_key == scope_key).one()
            assert row.status == "failed"
            assert row.last_error == scheduler_notifications._PROXY_INTERCEPTED_ERROR
            assert "smtp.qq.com" in row.last_error
            assert "EOF occurred" not in row.last_error
    finally:
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.clear()


def test_next_normal_manual_batch_retries_older_failed_report(session_factory):
    old_scope = "group:8:run:old"
    new_scope = "group:8:run:new"
    old_entry = {
        "task_id": 8,
        "task_name": "Old Store",
        "scope_key": old_scope,
        "scope_label": "分组 8",
        "scope_kind": "group",
        "new_products": [],
        "removed_products": [],
        "current_products": [],
        "status": "failed",
        "recipients": ["reports@example.com"],
    }
    new_entry = {**old_entry, "task_id": 9, "task_name": "New Store", "scope_key": new_scope}

    with scheduler_notifications._DIGEST_LOCK:
        scheduler_notifications._PENDING_DIGEST.clear()
        scheduler_notifications._PENDING_DIGEST.append(old_entry)
    try:
        with patch("scheduler_notifications.send_consolidated_email", return_value=False):
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={old_scope})

        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.append(new_entry)

        delivered_scopes = []

        def _capture(_db, entries, retry_attempts=2):
            delivered_scopes.append(entries[0]["scope_key"])
            return True

        with patch.object(scheduler, "SessionLocal", session_factory), patch(
            "scheduler_notifications.send_consolidated_email", side_effect=_capture
        ):
            scheduler._flush_digest({new_scope})

        assert delivered_scopes == [old_scope, new_scope]
        with session_factory() as db:
            statuses = {
                row.scope_key: row.status
                for row in db.query(ReportOutbox).order_by(ReportOutbox.id).all()
            }
        assert statuses == {old_scope: "sent", new_scope: "sent"}
    finally:
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.clear()


def test_manual_review_holds_full_group_and_sends_reminder_only(session_factory):
    scope_key = "group:41:run:review-only"
    with session_factory() as db:
        group = Category(name="待确认分组")
        db.add(group)
        db.commit()
        normal = _task("Normal Store", group.id)
        pending = _task("Pending Store", group.id)
        pending.manual_review_pending = True
        pending.manual_review_kind = "failure"
        pending.manual_review_scope_key = scope_key
        pending.manual_review_scope_label = group.name
        pending.manual_review_scope_kind = "group"
        pending.last_failure_reason = "需要确认抓取数量"
        db.add_all([normal, pending])
        db.commit()
        db.refresh(normal)
        db.refresh(pending)
        entries = [
            _report_entry(
                normal,
                scope_key,
                status="success",
                products=[{"name": "Normal Product", "link": "https://example.com/normal-product"}],
            ),
            _report_entry(pending, scope_key, status="failed"),
        ]

    with scheduler_notifications._DIGEST_LOCK:
        scheduler_notifications._PENDING_DIGEST.clear()
        scheduler_notifications._PENDING_DIGEST.extend(entries)
    captured: list[list[dict]] = []
    try:
        with patch(
            "scheduler_notifications.send_manual_review_reminder",
            side_effect=lambda _db, reminder_entries, retry_attempts=2: captured.append(reminder_entries) or True,
        ), patch("scheduler_notifications.send_consolidated_email") as complete_send:
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={scope_key})

        complete_send.assert_not_called()
        assert len(captured) == 1
        assert [entry["task_name"] for entry in captured[0]] == ["Pending Store"]
        reminder_json = scheduler_notifications._serialize_digest_entries(captured[0])
        assert "Normal Store" not in reminder_json
        assert "normal-product" not in reminder_json

        with session_factory() as db:
            full = db.query(ReportOutbox).filter(ReportOutbox.scope_key == scope_key).one()
            reminder = db.query(ReportOutbox).filter(
                ReportOutbox.scope_kind == "review_reminder"
            ).one()
            assert full.status == "held_review"
            assert len(scheduler_notifications._deserialize_digest_entries(full)) == 2
            assert reminder.status == "sent"
            assert [
                entry["task_name"]
                for entry in scheduler_notifications._deserialize_digest_entries(reminder)
            ] == ["Pending Store"]
    finally:
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.clear()


def test_complete_group_report_waits_for_every_review_then_sends_all_stores(session_factory):
    scope_key = "group:42:run:two-reviews"
    with session_factory() as db:
        group = Category(name="两项确认分组")
        db.add(group)
        db.commit()
        normal = _task("Normal Store", group.id)
        first = _task("First Review", group.id)
        second = _task("Second Review", group.id)
        for task, reason in ((first, "数量过低"), (second, "目录变化")):
            task.manual_review_pending = True
            task.manual_review_kind = "integrity"
            task.manual_review_scope_key = scope_key
            task.manual_review_scope_label = group.name
            task.manual_review_scope_kind = "group"
            task.last_failure_reason = reason
        db.add_all([normal, first, second])
        db.commit()
        for task in (normal, first, second):
            db.refresh(task)
        task_ids = (first.id, second.id)
        entries = [
            _report_entry(normal, scope_key, status="success", products=[{"name": "N", "link": "https://example.com/n"}]),
            _report_entry(first, scope_key, status="failed"),
            _report_entry(second, scope_key, status="failed"),
        ]

    with scheduler_notifications._DIGEST_LOCK:
        scheduler_notifications._PENDING_DIGEST.clear()
        scheduler_notifications._PENDING_DIGEST.extend(entries)
    final_payloads: list[list[dict]] = []
    try:
        with patch("scheduler_notifications.send_manual_review_reminder", return_value=True), patch(
            "scheduler_notifications.send_consolidated_email",
            side_effect=lambda _db, final_entries, retry_attempts=2: final_payloads.append(final_entries) or True,
        ):
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={scope_key})
            with session_factory() as db:
                first = db.query(MonitorTask).filter(MonitorTask.id == task_ids[0]).one()
                first.manual_review_pending = False
                db.add(ProductItem(task_id=first.id, name="First Product", product_link="https://example.com/first"))
                db.commit()
            first_result = scheduler_notifications.resolve_held_report_task(
                session_factory,
                scope_key,
                task_ids[0],
                resolution_message="已确认第一项",
            )
            assert first_result["remaining"] == 1
            assert first_result["released"] is False
            assert final_payloads == []

            with patch("scheduler_notifications.send_manual_review_reminder") as repeated_reminder:
                retry_result = scheduler_notifications.retry_pending_reports(session_factory)
            assert retry_result == {"attempted": 0, "sent": 0, "failed": 0}
            repeated_reminder.assert_not_called()

            with session_factory() as db:
                second = db.query(MonitorTask).filter(MonitorTask.id == task_ids[1]).one()
                second.manual_review_pending = False
                db.add(ProductItem(task_id=second.id, name="Second Product", product_link="https://example.com/second"))
                db.commit()
            second_result = scheduler_notifications.resolve_held_report_task(
                session_factory,
                scope_key,
                task_ids[1],
                resolution_message="已确认第二项",
            )

        assert second_result["released"] is True
        assert second_result["delivered"] is True
        assert len(final_payloads) == 1
        final_by_name = {entry["task_name"]: entry for entry in final_payloads[0]}
        assert set(final_by_name) == {"Normal Store", "First Review", "Second Review"}
        assert final_by_name["First Review"]["status"] == "confirmed"
        assert final_by_name["Second Review"]["status"] == "confirmed"
        assert final_by_name["First Review"]["current_products"][0]["link"].endswith("/first")
        assert final_by_name["Second Review"]["current_products"][0]["link"].endswith("/second")
    finally:
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.clear()


def test_startup_reconcile_does_not_repeat_reminder_and_releases_ready_report(session_factory):
    scope_key = "group:43:run:restart"
    with session_factory() as db:
        group = Category(name="重启恢复分组")
        db.add(group)
        db.commit()
        pending = _task("Restart Review", group.id)
        pending.manual_review_pending = True
        pending.manual_review_kind = "failure"
        pending.manual_review_scope_key = scope_key
        pending.manual_review_scope_label = group.name
        pending.manual_review_scope_kind = "group"
        pending.last_failure_reason = "网络失败"
        db.add(pending)
        db.commit()
        db.refresh(pending)
        task_id = pending.id
        entry = _report_entry(pending, scope_key, status="failed")

    with scheduler_notifications._DIGEST_LOCK:
        scheduler_notifications._PENDING_DIGEST.clear()
        scheduler_notifications._PENDING_DIGEST.append(entry)
    try:
        with patch("scheduler_notifications.send_manual_review_reminder", return_value=True):
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={scope_key})

        with patch("scheduler_notifications.send_manual_review_reminder") as duplicate_reminder, patch(
            "scheduler_notifications.send_consolidated_email"
        ) as premature_final:
            result = scheduler_notifications.retry_pending_reports(session_factory)
        assert result == {"attempted": 0, "sent": 0, "failed": 0}
        duplicate_reminder.assert_not_called()
        premature_final.assert_not_called()

        with session_factory() as db:
            task = db.query(MonitorTask).filter(MonitorTask.id == task_id).one()
            task.manual_review_pending = False
            db.commit()

        with patch("scheduler_notifications.send_consolidated_email", return_value=True) as final_send:
            result = scheduler_notifications.retry_pending_reports(session_factory)
        assert result == {"attempted": 1, "sent": 1, "failed": 0}
        final_send.assert_called_once()
        with session_factory() as db:
            full = db.query(ReportOutbox).filter(ReportOutbox.scope_key == scope_key).one()
            assert full.status == "sent"
    finally:
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.clear()


def test_retry_success_merges_back_into_held_group_before_final_send(session_factory):
    scope_key = "group:44:run:retry-merge"
    with session_factory() as db:
        group = Category(name="重试合并分组")
        db.add(group)
        db.commit()
        normal = _task("Kept Normal Store", group.id)
        retrying = _task("Retried Store", group.id)
        retrying.manual_review_pending = True
        retrying.manual_review_kind = "failure"
        retrying.manual_review_scope_key = scope_key
        retrying.manual_review_scope_label = group.name
        retrying.manual_review_scope_kind = "group"
        retrying.last_failure_reason = "首次失败"
        db.add_all([normal, retrying])
        db.commit()
        db.refresh(normal)
        db.refresh(retrying)
        retry_task_id = retrying.id
        initial_entries = [
            _report_entry(
                normal,
                scope_key,
                status="success",
                products=[{"name": "Kept", "link": "https://example.com/kept"}],
            ),
            _report_entry(retrying, scope_key, status="failed"),
        ]

    with scheduler_notifications._DIGEST_LOCK:
        scheduler_notifications._PENDING_DIGEST.clear()
        scheduler_notifications._PENDING_DIGEST.extend(initial_entries)
    final_payloads: list[list[dict]] = []
    try:
        with patch("scheduler_notifications.send_manual_review_reminder", return_value=True):
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={scope_key})

        with session_factory() as db:
            retrying = db.query(MonitorTask).filter(MonitorTask.id == retry_task_id).one()
            retrying.manual_review_pending = False
            db.commit()
            replacement = _report_entry(
                retrying,
                scope_key,
                status="success",
                products=[{"name": "Retry Product", "link": "https://example.com/retry-product"}],
            )
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.append(replacement)

        with patch.object(scheduler, "SessionLocal", session_factory), patch(
            "scheduler_notifications.send_consolidated_email",
            side_effect=lambda _db, entries, retry_attempts=2: final_payloads.append(entries) or True,
        ):
            scheduler._flush_digest({scope_key})

        assert len(final_payloads) == 1
        by_name = {entry["task_name"]: entry for entry in final_payloads[0]}
        assert set(by_name) == {"Kept Normal Store", "Retried Store"}
        assert by_name["Kept Normal Store"]["current_products"][0]["link"].endswith("/kept")
        assert by_name["Retried Store"]["current_products"][0]["link"].endswith("/retry-product")
    finally:
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.clear()


def test_resolving_review_cancels_failed_stale_reminder_before_final(session_factory):
    scope_key = "group:45:run:stale-reminder"
    with session_factory() as db:
        group = Category(name="提醒取消分组")
        db.add(group)
        db.commit()
        pending = _task("Quickly Confirmed Store", group.id)
        pending.manual_review_pending = True
        pending.manual_review_kind = "failure"
        pending.manual_review_scope_key = scope_key
        pending.manual_review_scope_label = group.name
        pending.manual_review_scope_kind = "group"
        pending.last_failure_reason = "待确认"
        db.add(pending)
        db.commit()
        db.refresh(pending)
        task_id = pending.id
        entry = _report_entry(pending, scope_key, status="failed")

    with scheduler_notifications._DIGEST_LOCK:
        scheduler_notifications._PENDING_DIGEST.clear()
        scheduler_notifications._PENDING_DIGEST.append(entry)
    try:
        with patch("scheduler_notifications.send_manual_review_reminder", return_value=False):
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={scope_key})
        with session_factory() as db:
            reminder = db.query(ReportOutbox).filter(
                ReportOutbox.scope_kind == "review_reminder"
            ).one()
            assert reminder.status == "failed"
            task = db.query(MonitorTask).filter(MonitorTask.id == task_id).one()
            task.manual_review_pending = False
            db.commit()

        with patch("scheduler_notifications.send_consolidated_email", return_value=True):
            result = scheduler_notifications.resolve_held_report_task(
                session_factory,
                scope_key,
                task_id,
            )
        assert result["delivered"] is True
        with session_factory() as db:
            reminder = db.query(ReportOutbox).filter(
                ReportOutbox.scope_kind == "review_reminder"
            ).one()
            assert reminder.status == "cancelled"
            assert reminder.payload_json == "[]"
    finally:
        with scheduler_notifications._DIGEST_LOCK:
            scheduler_notifications._PENDING_DIGEST.clear()


def test_late_failed_retry_keeps_complete_report_after_ignore_and_delivery_failure(session_factory):
    scope_key = "group:46:run:late-retry"
    with session_factory() as db:
        group = Category(name="迟到重试分组")
        db.add(group)
        db.flush()
        normal = _task("Normal Store", group.id)
        retrying = _task("Ignored Store", group.id)
        retrying.manual_review_pending = True
        retrying.manual_review_scope_key = scope_key
        db.add_all([normal, retrying])
        db.commit()
        retry_task_id = retrying.id
        normal_entry = _report_entry(
            normal, scope_key, status="success",
            products=[{"name": "Kept", "link": "https://example.com/kept"}],
        )
        normal_entry["new_products"] = list(normal_entry["current_products"])
        failed_entry = _report_entry(retrying, scope_key, status="failed")

    with patch.object(scheduler_notifications, "_PENDING_DIGEST", [normal_entry, failed_entry]):
        with patch("scheduler_notifications.send_manual_review_reminder", return_value=True):
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={scope_key})

        with session_factory() as db:
            db.get(MonitorTask, retry_task_id).manual_review_pending = False
            db.commit()

        with patch("scheduler_notifications.send_consolidated_email", return_value=False):
            result = scheduler_notifications.resolve_held_report_task(
                session_factory, scope_key, retry_task_id,
                resolution_message="人工处理：已忽略本次结果",
            )
        assert result["released"] and not result["delivered"]

        with session_factory() as db:
            row = db.query(ReportOutbox).filter_by(scope_key=scope_key).one()
            assert row.status == "failed"
            original_payload = json.loads(row.payload_json)

        scheduler_notifications._PENDING_DIGEST.append(failed_entry)
        with patch("scheduler_notifications.send_consolidated_email", return_value=True) as send:
            scheduler_notifications.flush_digest(session_factory, only_scope_keys={scope_key})

        send.assert_called_once()
        assert send.call_args.args[1] == original_payload
        assert original_payload[0] == normal_entry
        assert original_payload[1]["status"] == "confirmed"
        assert original_payload[1]["status_message"] == "人工处理：已忽略本次结果"
