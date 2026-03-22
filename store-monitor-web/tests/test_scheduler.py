import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

    def test_prune_removed_products_history_only_deletes_expired_removed_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_file = Path(tmp_dir) / "test_monitor.db"
            engine = create_engine(
                f"sqlite:///{db_file.as_posix()}",
                connect_args={"check_same_thread": False},
            )
            TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            Base.metadata.create_all(bind=engine)

            now = datetime.datetime.now(datetime.timezone.utc)
            with TestingSessionLocal() as db:
                db.add(SystemConfig(product_retention_days=30))
                task = MonitorTask(
                    name="demo",
                    url="https://example.com",
                    selector="div.item",
                    check_interval_hours=24,
                    recipients="a@test.com",
                    is_active=True,
                )
                db.add(task)
                db.flush()
                db.add_all(
                    [
                        ProductItem(
                            task_id=task.id,
                            product_link="https://example.com/p-removed-old",
                            name="old removed",
                            discovered_at=now - datetime.timedelta(days=50),
                            removed_at=now - datetime.timedelta(days=40),
                        ),
                        ProductItem(
                            task_id=task.id,
                            product_link="https://example.com/p-removed-fresh",
                            name="fresh removed",
                            discovered_at=now - datetime.timedelta(days=15),
                            removed_at=now - datetime.timedelta(days=10),
                        ),
                        ProductItem(
                            task_id=task.id,
                            product_link="https://example.com/p-active",
                            name="active",
                            discovered_at=now - datetime.timedelta(days=120),
                            removed_at=None,
                        ),
                    ]
                )
                db.commit()

            original_session_local = scheduler.SessionLocal
            try:
                scheduler.SessionLocal = TestingSessionLocal
                scheduler.prune_removed_products_history()
            finally:
                scheduler.SessionLocal = original_session_local

            try:
                with TestingSessionLocal() as db:
                    links = {item.product_link for item in db.query(ProductItem).all()}

                self.assertNotIn("https://example.com/p-removed-old", links)
                self.assertIn("https://example.com/p-removed-fresh", links)
                self.assertIn("https://example.com/p-active", links)
            finally:
                engine.dispose()

    def test_first_successful_run_sends_initial_digest_email(self):
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
                     patch("scheduler.send_email") as send_email_mock:
                    scheduler._execute_monitor_task_locked(task_id)
                    self.assertTrue(send_email_mock.called)
                    args = send_email_mock.call_args.args
                    self.assertEqual(len(args[2]), 1)  # new_products payload for initial digest
            finally:
                scheduler.SessionLocal = original_session_local
                engine.dispose()

    def test_transient_scrape_failure_is_requeued_without_health_penalty(self):
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
                     patch("scheduler._TASK_RETRY_ATTEMPTS", 0), \
                     patch("scheduler.time.sleep"), \
                     patch("scheduler.add_to_retry_queue") as retry_queue_mock, \
                     patch(
                         "scheduler.fetch_products_for_task",
                         side_effect=scheduler.ScrapeTransientError("captcha"),
                     ):
                    scheduler._execute_monitor_task_locked(task_id)

                with TestingSessionLocal() as db:
                    task = db.query(MonitorTask).filter(MonitorTask.id == task_id).first()
                    self.assertEqual(task.consecutive_empty_count, 0)
                    self.assertEqual(task.health_state, "healthy")

                retry_queue_mock.assert_called_once_with(task_id)
            finally:
                scheduler.SessionLocal = original_session_local
                engine.dispose()

    def test_network_retry_status_popup_event_is_throttled_until_recovery(self):
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

            with patch("scheduler._check_network_health", return_value=True), \
                 patch("scheduler.queue_monitor_task") as requeue_mock:
                scheduler._network_check_and_retry()

            recovered_status = scheduler.get_network_retry_status()
            self.assertFalse(recovered_status["alert_active"])
            self.assertEqual(recovered_status["pending_count"], 0)
            self.assertTrue(recovered_status["network_healthy"])
            self.assertEqual(recovered_status["recovery_event_id"], 1)
            self.assertIn("自动重试", recovered_status["last_recovery_message"])
            self.assertEqual(requeue_mock.call_count, 2)

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


if __name__ == "__main__":
    unittest.main()
