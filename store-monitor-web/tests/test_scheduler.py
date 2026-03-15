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


if __name__ == "__main__":
    unittest.main()
