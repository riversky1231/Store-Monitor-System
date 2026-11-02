import datetime
import tempfile
import unittest
from email.mime.text import MIMEText
from pathlib import Path
from unittest.mock import patch

import scheduler_notifications
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import scheduler
from database import Base
from models import MonitorTask, ProductItem, SystemConfig


class SchedulerTests(unittest.TestCase):
    def test_parse_recipients_filters_invalid_and_deduplicates(self):
        raw = "a@test.com,invalid-email,a@test.com,b@test.com,\r\nbad@test.com"
        actual = scheduler._parse_recipients(raw)
        self.assertEqual(actual, ["a@test.com", "b@test.com"])

    def test_smtp_failover_uses_qq_first_then_gmail_fallback(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            with TestingSessionLocal() as db:
                db.add(
                    SystemConfig(
                        smtp_server="smtp.qq.com",
                        smtp_port=465,
                        sender_email="primary@qq.com",
                        sender_password="enc::primary",
                        backup_smtp_enabled=True,
                        backup_smtp_server="smtp.gmail.com",
                        backup_smtp_port=587,
                        backup_sender_email="backup@gmail.com",
                        backup_sender_password="enc::backup",
                    )
                )
                db.commit()

                attempts = []

                def _fake_smtp_send(config, smtp_password, msg, recipients, retry_attempts=2):
                    attempts.append((config.smtp_server, smtp_password, msg["From"], list(recipients)))
                    if config.smtp_server == "smtp.qq.com":
                        raise TimeoutError("qq network timeout")

                msg = MIMEText("body", "plain", "utf-8")
                msg["Subject"] = "test"
                msg["From"] = "placeholder@example.com"
                msg["To"] = "ops@example.com"

                with patch("scheduler_notifications.decrypt_secret", side_effect=lambda value: value.replace("enc::", "")), \
                     patch("scheduler_notifications.smtp_send", side_effect=_fake_smtp_send):
                    ok = scheduler_notifications.send_with_smtp_failover(
                        db,
                        msg,
                        ["ops@example.com"],
                        retry_attempts=0,
                        purpose="unit-test",
                    )

                self.assertTrue(ok)
                self.assertEqual(
                    attempts,
                    [
                        ("smtp.qq.com", "primary", "primary@qq.com", ["ops@example.com"]),
                        ("smtp.gmail.com", "backup", "backup@gmail.com", ["ops@example.com"]),
                    ],
                )
                self.assertEqual(msg["From"], "backup@gmail.com")

            engine.dispose()

    def test_first_successful_run_queues_manual_report_with_all_current_products(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            with TestingSessionLocal() as db:
                db.add(
                    SystemConfig(
                        sender_email="sender@example.com",
                        sender_password="enc::dummy",
                    )
                )
                task = MonitorTask(
                    name="demo",
                    url="https://example.com",
                    selector="div.item",
                    check_interval_hours=24,
                    recipients="a@test.com",
                    is_active=True,
                    last_run_at=None,
                )
                db.add(task)
                db.commit()
                task_id = task.id

            original_session_local = scheduler.SessionLocal
            try:
                scheduler.SessionLocal = TestingSessionLocal
                with patch("scheduler.validate_monitor_target_url", return_value="https://example.com"), \
                     patch(
                         "scheduler.fetch_products_for_task",
                         return_value=(
                             [{"name": "P1", "link": "https://example.com/p1"}],
                             [],
                             [],
                         ),
                     ), \
                     patch("scheduler._notifications.send_consolidated_email", return_value=True) as send_report_mock:
                    scheduler._execute_monitor_task_locked(task_id)
                    send_report_mock.assert_called_once()
                    entries = send_report_mock.call_args.args[1]
                    self.assertEqual(len(entries), 1)
                    self.assertTrue(entries[0]["is_baseline"])
                    self.assertEqual(entries[0]["current_products"][0]["link"], "https://example.com/p1")
            finally:
                scheduler.SessionLocal = original_session_local
                engine.dispose()

    def test_transient_scrape_failure_requires_manual_review(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            with TestingSessionLocal() as db:
                task = MonitorTask(
                    name="transient-demo",
                    url="https://example.com",
                    selector="div.item",
                    check_interval_hours=24,
                    recipients="a@test.com",
                    is_active=True,
                    consecutive_empty_count=0,
                    health_state="healthy",
                )
                db.add(task)
                db.commit()
                task_id = task.id

            original_session_local = scheduler.SessionLocal
            try:
                scheduler.SessionLocal = TestingSessionLocal
                with patch("scheduler.validate_monitor_target_url", return_value="https://example.com"), \
                     patch("scheduler.add_to_retry_queue") as retry_queue_mock, \
                     patch(
                         "scheduler.fetch_products_for_task",
                         side_effect=scheduler.ScrapeTransientError("captcha"),
                     ):
                    scheduler._execute_monitor_task_locked(task_id)

                with TestingSessionLocal() as db:
                    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
                    self.assertEqual(task.consecutive_empty_count, 0)
                    self.assertEqual(task.consecutive_failure_count, 1)
                    self.assertEqual(task.health_state, "warning")
                    self.assertTrue(task.manual_review_pending)
                    self.assertEqual(task.manual_review_kind, "failure")
                    self.assertIn("captcha", task.last_failure_reason)

                retry_queue_mock.assert_not_called()
            finally:
                scheduler.SessionLocal = original_session_local
                engine.dispose()

    def test_pending_manual_review_scopes_excludes_inactive_tasks(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            with TestingSessionLocal() as db:
                db.add_all(
                    [
                        MonitorTask(
                            name="active-manual",
                            url="https://example.com/active-manual",
                            selector="div.item",
                            check_interval_hours=24,
                            recipients="a@test.com",
                            is_active=True,
                            manual_review_pending=True,
                            manual_review_scope_key="group:active",
                        ),
                        MonitorTask(
                            name="inactive-manual",
                            url="https://example.com/inactive-manual",
                            selector="div.item",
                            check_interval_hours=24,
                            recipients="b@test.com",
                            is_active=False,
                            manual_review_pending=True,
                            manual_review_scope_key="group:inactive",
                        ),
                    ]
                )
                db.commit()

            original_session_local = scheduler.SessionLocal
            try:
                scheduler.SessionLocal = TestingSessionLocal
                scopes = scheduler._pending_manual_review_scopes()
            finally:
                scheduler.SessionLocal = original_session_local
                engine.dispose()

            self.assertEqual(scopes, {"group:active"})

    def test_network_retry_status_popup_event_reports_manual_follow_up_after_recovery(self):
        original_queue = scheduler._network_retry_queue
        original_healthy = scheduler._network_healthy
        original_last_check = scheduler._last_network_check
        original_issue_active = scheduler._network_issue_active
        original_issue_event_id = scheduler._network_issue_event_id
        original_issue_at = scheduler._last_network_issue_at
        original_issue_message = scheduler._last_network_issue_message
        original_recovery_event_id = scheduler._network_recovery_event_id
        original_recovery_at = scheduler._last_network_recovery_at
        original_recovery_message = scheduler._last_network_recovery_message

        try:
            scheduler._network_retry_queue = []
            scheduler._network_healthy = True
            scheduler._last_network_check = None
            scheduler._network_issue_active = False
            scheduler._network_issue_event_id = 0
            scheduler._last_network_issue_at = None
            scheduler._last_network_issue_message = ""
            scheduler._network_recovery_event_id = 0
            scheduler._last_network_recovery_at = None
            scheduler._last_network_recovery_message = ""

            with patch("scheduler._ensure_network_check_scheduled"):
                scheduler.add_to_retry_queue(101)
                first_status = scheduler.get_network_retry_status()
                self.assertTrue(first_status["alert_active"])
                self.assertEqual(first_status["pending_count"], 1)
                self.assertEqual(first_status["alert_event_id"], 1)

                scheduler.add_to_retry_queue(202)
                second_status = scheduler.get_network_retry_status()
                self.assertEqual(second_status["alert_event_id"], 1)
                self.assertEqual(second_status["pending_count"], 2)

            with patch(
                "scheduler.run_network_diagnostics",
                return_value={
                    "success": True,
                    "message": "ok",
                    "response_time_ms": 123,
                    "details": [],
                },
            ), \
                 patch("scheduler.queue_monitor_task") as requeue_mock:
                scheduler._network_check_and_retry()

            recovered_status = scheduler.get_network_retry_status()
            self.assertFalse(recovered_status["alert_active"])
            self.assertEqual(recovered_status["pending_count"], 2)
            self.assertTrue(recovered_status["network_healthy"])
            self.assertEqual(recovered_status["recovery_event_id"], 1)
            self.assertIn("人工重试", recovered_status["last_recovery_message"])
            self.assertEqual(requeue_mock.call_count, 0)

            with patch("scheduler._ensure_network_check_scheduled"):
                scheduler.add_to_retry_queue(303)
            third_status = scheduler.get_network_retry_status()
            self.assertTrue(third_status["alert_active"])
            self.assertEqual(third_status["alert_event_id"], 2)
        finally:
            scheduler._network_retry_queue = original_queue
            scheduler._network_healthy = original_healthy
            scheduler._last_network_check = original_last_check
            scheduler._network_issue_active = original_issue_active
            scheduler._network_issue_event_id = original_issue_event_id
            scheduler._last_network_issue_at = original_issue_at
            scheduler._last_network_issue_message = original_issue_message
            scheduler._network_recovery_event_id = original_recovery_event_id
            scheduler._last_network_recovery_at = original_recovery_at
            scheduler._last_network_recovery_message = original_recovery_message

    def test_manual_network_check_keeps_pending_tasks_for_manual_retry(self):
        original_queue = scheduler._network_retry_queue
        original_healthy = scheduler._network_healthy
        original_last_check = scheduler._last_network_check
        original_issue_active = scheduler._network_issue_active

        try:
            scheduler._network_retry_queue = [11, 22]
            scheduler._network_healthy = False
            scheduler._last_network_check = None
            scheduler._network_issue_active = True

            with patch(
                "scheduler.run_network_diagnostics",
                return_value={
                    "success": True,
                    "message": "✅ 网络状态良好！平均响应 200ms，适合抓取。",
                    "response_time_ms": 200,
                    "details": [],
                },
            ), patch("scheduler.queue_monitor_task") as queue_mock:
                payload = scheduler.run_manual_network_check()

            self.assertTrue(payload["success"])
            self.assertIn("待人工处理任务", payload["message"])
            self.assertEqual(scheduler.get_retry_queue_snapshot(), [11, 22])
            self.assertEqual(queue_mock.call_count, 0)
        finally:
            scheduler._network_retry_queue = original_queue
            scheduler._network_healthy = original_healthy
            scheduler._last_network_check = original_last_check
            scheduler._network_issue_active = original_issue_active

    def test_schedule_task_skips_auto_scheduling_when_manual_review_pending(self):
        task = MonitorTask(
            id=321,
            name="pending-review-task",
            url="https://example.com",
            selector="div.item",
            check_interval_hours=24,
            recipients="a@test.com",
            is_active=True,
            manual_review_pending=True,
        )

        with patch.object(scheduler.scheduler, "get_job", return_value=None), \
             patch.object(scheduler.scheduler, "remove_job") as remove_job_mock, \
             patch.object(scheduler.scheduler, "add_job") as add_job_mock:
            scheduler.schedule_task(task)

        remove_job_mock.assert_not_called()
        add_job_mock.assert_not_called()

    def test_daily_status_report_updates_last_sent_on_success(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            with TestingSessionLocal() as db:
                db.add(
                    SystemConfig(
                        sender_email="sender@example.com",
                        sender_password="enc::dummy",
                    )
                )
                db.add(
                    MonitorTask(
                        name="daily-demo",
                        url="https://example.com",
                        selector="div.item",
                        check_interval_hours=24,
                        recipients="a@test.com",
                        is_active=True,
                    )
                )
                db.commit()

            original_session_local = scheduler.SessionLocal
            try:
                scheduler.SessionLocal = TestingSessionLocal
                with patch("scheduler._send_daily_status_email", return_value=True):
                    scheduler.send_daily_status_report()

                with TestingSessionLocal() as db:
                    config = db.query(SystemConfig).first()
                    self.assertIsNotNone(config.last_daily_report_at)
            finally:
                scheduler.SessionLocal = original_session_local
                engine.dispose()

    def test_daily_status_report_skip_uses_local_report_date(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            with TestingSessionLocal() as db:
                db.add(
                    SystemConfig(
                        sender_email="sender@example.com",
                        sender_password="enc::dummy",
                        last_daily_report_at=datetime.datetime(2026, 4, 1, 16, 30),
                    )
                )
                db.add(
                    MonitorTask(
                        name="daily-demo",
                        url="https://example.com",
                        selector="div.item",
                        check_interval_hours=24,
                        recipients="a@test.com",
                        is_active=True,
                    )
                )
                db.commit()

            original_session_local = scheduler.SessionLocal
            try:
                scheduler.SessionLocal = TestingSessionLocal
                with patch("scheduler._send_daily_status_email") as send_mock:
                    scheduler.send_daily_status_report(
                        now=datetime.datetime(2026, 4, 2, 1, 0, tzinfo=datetime.timezone.utc)
                    )
                send_mock.assert_not_called()
            finally:
                scheduler.SessionLocal = original_session_local
                engine.dispose()

    def test_daily_status_report_if_due_catches_missed_cron_after_report_time(self):
        with patch("scheduler.send_daily_status_report") as send_mock:
            scheduler.send_daily_status_report_if_due(
                now=datetime.datetime(2026, 4, 2, 2, 0, tzinfo=datetime.timezone.utc)
            )
        send_mock.assert_called_once()

    def test_daily_status_report_if_due_waits_before_report_time(self):
        with patch("scheduler.send_daily_status_report") as send_mock:
            scheduler.send_daily_status_report_if_due(
                now=datetime.datetime(2026, 4, 2, 0, 30, tzinfo=datetime.timezone.utc)
            )
        send_mock.assert_not_called()

    def test_daily_new_product_payload_uses_today_new_discoveries_only(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            with TestingSessionLocal() as db:
                task = MonitorTask(
                    name="daily-products",
                    url="https://example.com",
                    selector="div.item",
                    check_interval_hours=24,
                    recipients="a@test.com",
                    is_active=True,
                )
                db.add(task)
                db.commit()
                db.refresh(task)

                db.add_all(
                    [
                        ProductItem(
                            task_id=task.id,
                            product_link="https://example.com/today-new",
                            name="Today New",
                            discovered_at=datetime.datetime(2026, 4, 1, 1, 30),
                            is_new_discovery=True,
                        ),
                        ProductItem(
                            task_id=task.id,
                            product_link="https://example.com/today-baseline",
                            name="Today Baseline",
                            discovered_at=datetime.datetime(2026, 4, 1, 1, 45),
                            is_new_discovery=False,
                        ),
                        ProductItem(
                            task_id=task.id,
                            product_link="https://example.com/yesterday-new",
                            name="Yesterday New",
                            discovered_at=datetime.datetime(2026, 3, 31, 1, 30),
                            is_new_discovery=True,
                        ),
                    ]
                )
                db.commit()

                products, report_date = scheduler._daily_new_product_payload(
                    db,
                    datetime.datetime(2026, 4, 1, 9, 0, tzinfo=datetime.timezone.utc),
                )

            self.assertEqual(report_date, "2026-04-01")
            self.assertEqual([item["name"] for item in products], ["Today New"])
            engine.dispose()

    def test_legacy_scheduled_scope_no_longer_suppresses_manual_report(self):
        task = MonitorTask(id=123, name="scheduled-demo", category_id=None)
        try:
            scheduler._set_execution_context(123, scheduler._scheduled_digest_scope())
            with patch("scheduler._notifications.queue_digest_entry") as queue_mock:
                scheduler._queue_digest_entry(
                    task,
                    [{"name": "New Product", "link": "https://example.com/new"}],
                    [],
                )
            queue_mock.assert_called_once()
        finally:
            scheduler._clear_execution_context()

    def test_daily_status_email_omits_failed_task_details(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            with TestingSessionLocal() as db:
                db.add(
                    SystemConfig(
                        smtp_server="smtp.example.com",
                        smtp_port=465,
                        sender_email="sender@example.com",
                        sender_password="enc::dummy",
                    )
                )
                task = MonitorTask(
                    name="mail-demo",
                    url="https://example.com",
                    selector="div.item",
                    check_interval_hours=24,
                    recipients="a@test.com",
                    is_active=True,
                )
                db.add(task)
                db.commit()

                sent = {}

                def _fake_smtp_send(config, smtp_password, msg, recipients, retry_attempts=2):
                    sent["msg"] = msg
                    sent["recipients"] = recipients

                with patch("scheduler_notifications.decrypt_secret", return_value="secret"), \
                     patch("scheduler_notifications.smtp_send", side_effect=_fake_smtp_send):
                    ok = scheduler_notifications.send_daily_status_email(
                        db,
                        {
                            "generated_at": datetime.datetime(2026, 4, 1, 9, 0, tzinfo=datetime.timezone.utc),
                            "active_task_count": 1,
                            "retry_pending_count": 0,
                            "running_task_name": "",
                            "waiting_count": 0,
                            "abnormal_tasks": [],
                            "failed_tasks": [
                                {
                                    "task_id": task.id,
                                    "task_name": "Should Not Appear",
                                    "group_name": "Test Group",
                                    "status_text": "失败 5 次",
                                    "reason": "抓取结果为空",
                                    "updated_at": "2026-04-01 09:00",
                                }
                            ],
                        },
                    )

                self.assertTrue(ok)
                body = sent["msg"].get_payload(decode=True).decode("utf-8")
                self.assertIn("当前共有 <b>1</b> 个失败任务", body)
                self.assertIn("失败任务不会写入邮件详情", body)
                self.assertIn("今日暂未发现新品。", body)
                self.assertNotIn("Should Not Appear", body)
            engine.dispose()

    def test_daily_status_email_formats_abnormal_task_datetime(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            with TestingSessionLocal() as db:
                db.add(
                    SystemConfig(
                        smtp_server="smtp.example.com",
                        smtp_port=465,
                        sender_email="sender@example.com",
                        sender_password="enc::dummy",
                    )
                )
                db.add(
                    MonitorTask(
                        name="daily-mail-demo",
                        url="https://example.com",
                        selector="div.item",
                        check_interval_hours=24,
                        recipients="a@test.com",
                        is_active=True,
                    )
                )
                db.commit()

                sent = {}

                def _fake_smtp_send(config, smtp_password, msg, recipients, retry_attempts=2):
                    sent["msg"] = msg

                with patch("scheduler_notifications.decrypt_secret", return_value="secret"), \
                     patch("scheduler_notifications.smtp_send", side_effect=_fake_smtp_send):
                    ok = scheduler_notifications.send_daily_status_email(
                        db,
                        {
                            "generated_at": datetime.datetime(2026, 4, 1, 1, 0, tzinfo=datetime.timezone.utc),
                            "report_date_text": "2026-04-01",
                            "active_task_count": 1,
                            "retry_pending_count": 0,
                            "running_task_name": "",
                            "waiting_count": 0,
                            "abnormal_tasks": [
                                {
                                    "task_id": 1,
                                    "task_name": "Warning Task",
                                    "group_name": "Test Group",
                                    "status_text": "异常",
                                    "reason": "连续为空",
                                    "updated_at": datetime.datetime(2026, 4, 1, 1, 0, tzinfo=datetime.timezone.utc),
                                }
                            ],
                            "failed_tasks": [],
                            "new_products": [],
                        },
                    )

                self.assertTrue(ok)
                body = sent["msg"].get_payload(decode=True).decode("utf-8")
                self.assertIn("Warning Task", body)
                self.assertIn("2026-04-01 09:00", body)
            engine.dispose()

    def test_daily_status_email_lists_new_products(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            with TestingSessionLocal() as db:
                db.add(
                    SystemConfig(
                        smtp_server="smtp.example.com",
                        smtp_port=465,
                        sender_email="sender@example.com",
                        sender_password="enc::dummy",
                    )
                )
                db.add(
                    MonitorTask(
                        name="daily-mail-demo",
                        url="https://example.com",
                        selector="div.item",
                        check_interval_hours=24,
                        recipients="a@test.com",
                        is_active=True,
                    )
                )
                db.commit()

                sent = {}

                def _fake_smtp_send(config, smtp_password, msg, recipients, retry_attempts=2):
                    sent["msg"] = msg

                with patch("scheduler_notifications.decrypt_secret", return_value="secret"), \
                     patch("scheduler_notifications.smtp_send", side_effect=_fake_smtp_send):
                    ok = scheduler_notifications.send_daily_status_email(
                        db,
                        {
                            "generated_at": datetime.datetime(2026, 4, 1, 9, 0, tzinfo=datetime.timezone.utc),
                            "report_date_text": "2026-04-01",
                            "active_task_count": 1,
                            "retry_pending_count": 0,
                            "running_task_name": "",
                            "waiting_count": 0,
                            "abnormal_tasks": [],
                            "failed_tasks": [],
                            "new_products": [
                                {
                                    "name": "Launch Product",
                                    "link": "https://example.com/products/launch",
                                    "task_name": "daily-mail-demo",
                                    "group_name": "Test Group",
                                    "discovered_text": "04-01 17:30",
                                }
                            ],
                        },
                    )

                self.assertTrue(ok)
                body = sent["msg"].get_payload(decode=True).decode("utf-8")
                self.assertIn("今日新上架商品 (1)", body)
                self.assertIn("Launch Product", body)
                self.assertIn("daily-mail-demo", body)
                self.assertNotIn("今日暂未发现新品。", body)
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
