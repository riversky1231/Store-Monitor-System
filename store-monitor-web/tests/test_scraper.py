"""Unit tests for scraper._canonicalize_link and scraper._sync_products_to_db."""
import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from models import MonitorTask, ProductItem
from scraper import _canonicalize_link, _sync_products_to_db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(engine)


@pytest.fixture()
def task(db_session):
    t = MonitorTask(
        name="Test Task",
        url="https://www.amazon.com/s?me=A123456",
        selector='div[data-component-type="s-search-result"]',
        check_interval_hours=6,
        recipients="test@example.com",
        is_active=True,
    )
    db_session.add(t)
    db_session.commit()
    return t


# ---------------------------------------------------------------------------
# _canonicalize_link
# ---------------------------------------------------------------------------

class TestCanonicalizeLink:
    BASE = "https://www.amazon.com"

    def test_amazon_asin_extracted(self):
        link = _canonicalize_link(self.BASE, "/dp/B08N5LNQCX/ref=sr_1_1?ie=UTF8")
        assert link == "https://www.amazon.com/dp/B08N5LNQCX"

    def test_amazon_gp_product_normalized(self):
        link = _canonicalize_link(self.BASE, "/gp/product/B01DFKBL68/ref=sr_1_2")
        assert link == "https://www.amazon.com/dp/B01DFKBL68"

    def test_trailing_slash_stripped(self):
        link = _canonicalize_link("https://shop.example.com", "/products/widget/")
        assert not link.endswith("/")

    def test_query_params_stripped(self):
        link = _canonicalize_link("https://shop.example.com", "/products/widget?color=red&size=M")
        assert "color" not in link
        assert "size" not in link

    def test_relative_path_resolved(self):
        link = _canonicalize_link("https://shop.example.com", "/products/item-123")
        assert link == "https://shop.example.com/products/item-123"

    def test_absolute_url_unchanged(self):
        link = _canonicalize_link(self.BASE, "https://other.example.com/page")
        assert link == "https://other.example.com/page"

    def test_empty_raw_link_returns_empty(self):
        assert _canonicalize_link(self.BASE, "") == ""

    def test_none_raw_link_returns_empty(self):
        assert _canonicalize_link(self.BASE, None) == ""

    def test_asin_uppercased(self):
        link = _canonicalize_link(self.BASE, "/dp/b08n5lnqcx")
        assert "/dp/B08N5LNQCX" in link


# ---------------------------------------------------------------------------
# _sync_products_to_db
# ---------------------------------------------------------------------------

class TestSyncProductsToDB:
    PRODUCT_A = {"name": "Widget A", "link": "https://www.amazon.com/dp/AAAAAAAAA1"}
    PRODUCT_B = {"name": "Widget B", "link": "https://www.amazon.com/dp/BBBBBBBBB2"}
    PRODUCT_C = {"name": "Widget C", "link": "https://www.amazon.com/dp/CCCCCCCCC3"}
    NOISE_PRODUCT = {
        "name": "Amazon Business Card",
        "link": "https://www.amazon.com/dp/NOISE000001",
    }

    def test_first_run_seeds_baseline_no_alerts(self, db_session, task):
        current, new_prods, removed_prods = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B]
        )
        assert len(current) == 2
        assert new_prods == [], "First run must not report new products"
        assert removed_prods == [], "First run must not report removed products"
        assert db_session.query(ProductItem).filter_by(task_id=task.id).count() == 2

    def test_new_product_detected_on_second_run(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])
        _, new_prods, _ = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B]
        )
        assert len(new_prods) == 1
        assert new_prods[0]["link"] == self.PRODUCT_B["link"]

    def test_removed_product_flagged(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B])
        _, _, removed_prods = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A]
        )
        assert len(removed_prods) == 1
        assert removed_prods[0]["link"] == self.PRODUCT_B["link"]

        item = db_session.query(ProductItem).filter_by(
            product_link=self.PRODUCT_B["link"]
        ).first()
        assert item.removed_at is not None

    def test_restored_product_clears_removed_at(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B])
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])  # B removed

        item = db_session.query(ProductItem).filter_by(
            product_link=self.PRODUCT_B["link"]
        ).first()
        assert item.removed_at is not None

        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B])  # B restored
        db_session.refresh(item)
        assert item.removed_at is None

    def test_no_duplicate_inserts(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])
        count = db_session.query(ProductItem).filter_by(
            product_link=self.PRODUCT_A["link"]
        ).count()
        assert count == 1

    def test_empty_current_returns_empty_lists(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])
        current, new_prods, removed_prods = _sync_products_to_db(db_session, task.id, [])
        assert current == []
        assert new_prods == []
        assert removed_prods == []

    def test_noise_products_removed_from_db(self, db_session, task):
        db_session.add(ProductItem(
            task_id=task.id,
            product_link=self.NOISE_PRODUCT["link"],
            name=self.NOISE_PRODUCT["name"],
        ))
        db_session.commit()

        _, new_prods, removed_prods = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A]
        )
        assert len(new_prods) == 1
        assert new_prods[0]["link"] == self.PRODUCT_A["link"]
        assert removed_prods == []
        assert db_session.query(ProductItem).filter_by(name=self.NOISE_PRODUCT["name"]).count() == 0

    def test_task_not_found_returns_empty(self, db_session):
        from scraper import fetch_products_for_task
        # fetch_products_for_task handles the not-found case gracefully
        current, new_prods, removed_prods = _sync_products_to_db(db_session, 99999, [])
        assert current == new_prods == removed_prods == []
