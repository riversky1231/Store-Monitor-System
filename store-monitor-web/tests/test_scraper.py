"""Unit tests for scraper._canonicalize_link and scraper._sync_products_to_db."""
import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import scraper
from database import Base
from models import MonitorTask, ProductItem, SeenProduct
from scraper import (
    ScrapeIncomplete,
    _canonicalize_link,
    _required_integrity_ratio,
    _sync_products_to_db,
    accept_pending_integrity_review,
    get_pending_integrity_review,
)


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

        inserted = db_session.query(ProductItem).filter_by(
            task_id=task.id,
            product_link=self.PRODUCT_B["link"],
        ).first()
        assert inserted is not None
        assert inserted.discovered_at is not None

    def test_globally_known_product_is_stored_without_retriggering_new_alert(self, db_session, task):
        other_task = MonitorTask(
            name="Other Task",
            url="https://www.amazon.com/s?me=B654321",
            selector='div[data-component-type="s-search-result"]',
            check_interval_hours=6,
            recipients="other@example.com",
            is_active=True,
        )
        db_session.add(other_task)
        db_session.commit()
        db_session.refresh(other_task)

        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])
        _sync_products_to_db(db_session, other_task.id, [self.PRODUCT_B])

        current, new_prods, removed_prods = _sync_products_to_db(
            db_session,
            other_task.id,
            [self.PRODUCT_A, self.PRODUCT_B],
        )

        assert len(current) == 2
        assert new_prods == []
        assert removed_prods == []
        assert db_session.query(ProductItem).filter_by(task_id=other_task.id).count() == 2

    def test_removed_product_flagged(self, db_session, task, monkeypatch):
        monkeypatch.setattr(scraper, "_TRACK_REMOVALS", True)
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B])
        _, _, removed_prods = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A]
        )
        assert removed_prods == []

        _, _, removed_prods = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A]
        )
        assert len(removed_prods) == 1
        assert removed_prods[0]["link"] == self.PRODUCT_B["link"]

        item = db_session.query(ProductItem).filter_by(
            product_link=self.PRODUCT_B["link"]
        ).first()
        assert item.removed_at is not None

    def test_restored_product_clears_removed_at(self, db_session, task, monkeypatch):
        monkeypatch.setattr(scraper, "_TRACK_REMOVALS", True)
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B])
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])  # B removed

        item = db_session.query(ProductItem).filter_by(
            product_link=self.PRODUCT_B["link"]
        ).first()
        assert item.removed_at is not None

        current, new_prods, removed_prods = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B]
        )  # B restored
        db_session.refresh(item)
        assert len(current) == 2
        assert new_prods == []
        assert removed_prods == []
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

    def test_partial_scrape_for_small_store_is_rejected(self, db_session, task):
        baseline = [
            self.PRODUCT_A,
            self.PRODUCT_B,
            self.PRODUCT_C,
            {"name": "Widget D", "link": "https://www.amazon.com/dp/DDDDDDDDD4"},
            {"name": "Widget E", "link": "https://www.amazon.com/dp/EEEEEEEEE5"},
        ]
        _sync_products_to_db(db_session, task.id, baseline)

        with pytest.raises(ScrapeIncomplete):
            _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])

    def test_small_store_joint_threshold_accepts_three_of_five(self, db_session, task):
        baseline = [
            self.PRODUCT_A,
            self.PRODUCT_B,
            self.PRODUCT_C,
            {"name": "Widget D", "link": "https://www.amazon.com/dp/DDDDDDDDD4"},
            {"name": "Widget E", "link": "https://www.amazon.com/dp/EEEEEEEEE5"},
        ]
        partial = baseline[:3]

        _sync_products_to_db(db_session, task.id, baseline)

        current, new_prods, removed_prods = _sync_products_to_db(db_session, task.id, partial)

        db_session.refresh(task)
        assert len(current) == 3
        assert new_prods == []
        assert removed_prods == []
        assert task.integrity_reference_count == 5

    def test_joint_threshold_rejection_persists_pending_review_state(self, db_session, task, monkeypatch, tmp_path):
        state_path = tmp_path / "integrity-review.json"
        monkeypatch.setattr(scraper, "_integrity_review_state_path", lambda _task_id: state_path)

        baseline = [
            self.PRODUCT_A,
            self.PRODUCT_B,
            self.PRODUCT_C,
            {"name": "Widget D", "link": "https://www.amazon.com/dp/DDDDDDDDD4"},
            {"name": "Widget E", "link": "https://www.amazon.com/dp/EEEEEEEEE5"},
        ]
        _sync_products_to_db(db_session, task.id, baseline)

        with pytest.raises(ScrapeIncomplete):
            _sync_products_to_db(db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B])

        review = get_pending_integrity_review(task.id)
        assert state_path.exists()
        assert review is not None
        assert review["current_count"] == 2
        assert review["reference_count"] == 5
        assert review["min_required_count"] == 3

    def test_accept_pending_integrity_review_updates_reference_count(self, db_session, task, monkeypatch, tmp_path):
        state_path = tmp_path / "integrity-review.json"
        monkeypatch.setattr(scraper, "_integrity_review_state_path", lambda _task_id: state_path)

        baseline = [
            self.PRODUCT_A,
            self.PRODUCT_B,
            self.PRODUCT_C,
            {"name": "Widget D", "link": "https://www.amazon.com/dp/DDDDDDDDD4"},
            {"name": "Widget E", "link": "https://www.amazon.com/dp/EEEEEEEEE5"},
        ]
        current_partial = [self.PRODUCT_A, self.PRODUCT_B]

        _sync_products_to_db(db_session, task.id, baseline)

        with pytest.raises(ScrapeIncomplete):
            _sync_products_to_db(db_session, task.id, current_partial)

        task.manual_review_pending = True
        task.manual_review_kind = "integrity"
        task.manual_review_scope_key = "group:test"
        task.manual_review_scope_label = "测试分组"
        task.manual_review_scope_kind = "group"
        db_session.commit()

        result = accept_pending_integrity_review(db_session, task.id)

        db_session.refresh(task)
        active_count = db_session.query(ProductItem).filter(
            ProductItem.task_id == task.id,
            ProductItem.removed_at.is_(None),
        ).count()

        assert result == {"current_count": 2, "new_count": 0, "removed_count": 3}
        assert task.integrity_reference_count == 2
        assert task.health_state == "healthy"
        assert task.manual_review_pending is False
        assert task.manual_review_kind is None
        assert task.manual_review_scope_key is None
        assert task.last_failure_reason is None
        assert active_count == 2
        assert not state_path.exists()

    def test_large_store_uses_seventy_percent_integrity_threshold(self, db_session, task):
        baseline = [
            {"name": f"Widget {idx}", "link": f"https://www.amazon.com/dp/LARGE{idx:04d}"}
            for idx in range(100)
        ]
        accepted_partial = baseline[:70]
        rejected_partial = baseline[:69]

        _sync_products_to_db(db_session, task.id, baseline)

        current, new_prods, removed_prods = _sync_products_to_db(
            db_session, task.id, accepted_partial
        )

        assert len(current) == 70
        assert new_prods == []
        assert removed_prods == []
        assert _required_integrity_ratio(100) == pytest.approx(0.70)

        with pytest.raises(ScrapeIncomplete):
            _sync_products_to_db(db_session, task.id, rejected_partial)

    def test_duplicate_scraped_products_do_not_create_false_new_items(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])

        current, new_prods, removed_prods = _sync_products_to_db(
            db_session,
            task.id,
            [self.PRODUCT_A, dict(self.PRODUCT_A)],
        )

        assert len(current) == 1
        assert new_prods == []
        assert removed_prods == []

    def test_removed_products_are_ignored_by_default(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B])

        _, _, removed_first = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A]
        )
        _, _, removed_second = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A]
        )

        item = db_session.query(ProductItem).filter_by(
            product_link=self.PRODUCT_B["link"]
        ).first()
        assert removed_first == []
        assert removed_second == []
        assert item.removed_at is None
        assert item.miss_count == 0

    def test_high_change_catalog_shift_requires_confirmation(self, db_session, task, monkeypatch, tmp_path):
        state_path = tmp_path / "catalog-shift.json"
        monkeypatch.setattr(scraper, "_catalog_shift_state_path", lambda _task_id: state_path)

        baseline = [
            {"name": f"Baseline {idx}", "link": f"https://www.amazon.com/dp/BASLN000{idx:02d}"}
            for idx in range(12)
        ]
        changed = [
            {"name": f"Changed {idx}", "link": f"https://www.amazon.com/dp/CHANG000{idx:02d}"}
            for idx in range(12)
        ]

        _sync_products_to_db(db_session, task.id, baseline)

        with pytest.raises(ScrapeIncomplete):
            _sync_products_to_db(db_session, task.id, changed)

        assert state_path.exists()
        assert db_session.query(ProductItem).filter_by(task_id=task.id).count() == 12

        current, new_prods, removed_prods = _sync_products_to_db(db_session, task.id, changed)

        assert len(current) == 12
        assert len(new_prods) == 12
        assert removed_prods == []
        assert not state_path.exists()


class TestSeenProductsRegistry:
    """The permanent registry must keep "new product" alerts accurate for the
    lifetime of the system, surviving task deletion and retention pruning."""

    PRODUCT_A = {"name": "Widget A", "link": "https://www.amazon.com/dp/AAAAAAAAA1"}
    PRODUCT_B = {"name": "Widget B", "link": "https://www.amazon.com/dp/BBBBBBBBB2"}

    def _make_task(self, db_session, name="Registry Task"):
        t = MonitorTask(
            name=name,
            url="https://www.amazon.com/s?me=A999999",
            selector='div[data-component-type="s-search-result"]',
            check_interval_hours=6,
            recipients="test@example.com",
            is_active=True,
        )
        db_session.add(t)
        db_session.commit()
        db_session.refresh(t)
        return t

    def test_registry_populated_on_baseline_and_subsequent_runs(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B])

        links = {row.product_link for row in db_session.query(SeenProduct).all()}
        assert links == {self.PRODUCT_A["link"], self.PRODUCT_B["link"]}

    def test_registry_dedupes_repeated_sightings(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])
        assert db_session.query(SeenProduct).count() == 1

    def test_product_not_new_after_task_deleted_and_recreated(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B])

        # Simulate task deletion: cascade removes its ProductItem history.
        db_session.query(ProductItem).filter_by(task_id=task.id).delete()
        db_session.delete(task)
        db_session.commit()

        new_task = self._make_task(db_session)
        # First run on the new task is a baseline (no alerts by design).
        _sync_products_to_db(db_session, new_task.id, [self.PRODUCT_A])
        # Second run picks up PRODUCT_B again — it must NOT be reported as new.
        _, new_prods, _ = _sync_products_to_db(
            db_session, new_task.id, [self.PRODUCT_A, self.PRODUCT_B]
        )
        assert new_prods == []

    def test_pruned_then_relisted_product_not_reported_new(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B])

        # Simulate the retention job deleting the removed product's history row.
        db_session.query(ProductItem).filter_by(
            product_link=self.PRODUCT_B["link"]
        ).delete()
        db_session.commit()

        # Product B is re-listed later: stored again but no "new" alert.
        _, new_prods, _ = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A, self.PRODUCT_B]
        )
        assert new_prods == []
        assert (
            db_session.query(ProductItem)
            .filter_by(task_id=task.id, product_link=self.PRODUCT_B["link"])
            .count()
            == 1
        )

    def test_legacy_lowercase_asin_rows_still_match(self, db_session, task):
        _sync_products_to_db(db_session, task.id, [self.PRODUCT_A])
        # Simulate a legacy row stored with a lowercase ASIN.
        db_session.add(ProductItem(
            task_id=task.id,
            product_link="https://www.amazon.com/dp/cccccccccc",
            asin="cccccccc99",
            name="Legacy Widget",
        ))
        db_session.commit()

        item = {
            "name": "Legacy Widget",
            "link": "https://www.amazon.com/dp/CCCCCCCC99",
            "asin": "CCCCCCCC99",
        }
        _, new_prods, _ = _sync_products_to_db(
            db_session, task.id, [self.PRODUCT_A, item]
        )
        assert new_prods == []


def test_collect_products_does_not_poison_seen_asins_on_failed_title_extract(monkeypatch):
    class DummyElement:
        def get_attribute(self, name):
            if name == "data-asin":
                return "B012345678"
            return None

    class EmptyLocator:
        def all(self):
            return []

    class DummyPage:
        def locator(self, selector):
            return EmptyLocator()

    page = DummyPage()
    element = DummyElement()

    monkeypatch.setattr(scraper, "_get_elements", lambda *_args, **_kwargs: [element])
    monkeypatch.setattr(scraper, "_extract_link", lambda *_args, **_kwargs: "/dp/B012345678")
    monkeypatch.setattr(scraper, "_extract_title", lambda *_args, **_kwargs: "")

    seen_asins = set()
    seen_links = set()
    products = scraper._collect_products_from_page(
        page,
        "div[data-component-type='s-search-result']",
        "https://www.amazon.com",
        seen_asins,
        seen_links,
    )

    assert products == []
    assert seen_asins == set()
    assert seen_links == set()

    monkeypatch.setattr(scraper, "_extract_title", lambda *_args, **_kwargs: "Recovered product")
    products = scraper._collect_products_from_page(
        page,
        "div[data-component-type='s-search-result']",
        "https://www.amazon.com",
        seen_asins,
        seen_links,
    )

    assert len(products) == 1
    assert products[0]["asin"] == "B012345678"


def test_collect_storefront_products_uses_url_slug_when_anchor_has_no_title(monkeypatch):
    class DummyImgLocator:
        @property
        def first(self):
            return self

        def count(self):
            return 0

        def get_attribute(self, _name):
            return None

    class DummyAnchor:
        def __init__(self, href):
            self._href = href

        def get_attribute(self, name):
            if name == "href":
                return self._href
            return None

        def inner_text(self):
            return ""

        def locator(self, _selector):
            return DummyImgLocator()

    class DummyPageLocator:
        def __init__(self, anchors):
            self._anchors = anchors

        def all(self):
            return self._anchors

    class DummyPage:
        def __init__(self, anchors):
            self._anchors = anchors

        def locator(self, _selector):
            return DummyPageLocator(self._anchors)

    monkeypatch.setattr(scraper, "_add_partial_result", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_update_activity", lambda *_args, **_kwargs: None)

    href = "/VASAGLE-Square-Nightstand-Bedroom-ULET236K01/dp/B0DKDT3X4Z?ref_=ast_sto_dp"
    products = scraper._collect_asin_links_from_page(
        DummyPage([DummyAnchor(href)]),
        "https://www.amazon.com",
        set(),
    )

    assert len(products) == 1
    assert products[0]["asin"] == "B0DKDT3X4Z"
    assert products[0]["name"] == "VASAGLE Square Nightstand Bedroom ULET236K01"


def test_collect_storefront_products_from_html_uses_hidden_product_payload(monkeypatch):
    monkeypatch.setattr(scraper, "_add_partial_result", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scraper, "_update_activity", lambda *_args, **_kwargs: None)

    html_content = """
    <script type="application/json">
    {
      "products":[
        {
          "asin":"B0DKDT3X4Z",
          "detailPageLinkURL":"/VASAGLE-Square-Nightstand-Bedroom-ULET236K01/dp/B0DKDT3X4Z",
          "productImages":{"altText":"VASAGLE MAEZO Collection Side Table"}
        },
        {
          "asin":"B07984JN3L",
          "detailPageLinkURL":"/Amazon-Business-Card/dp/B07984JN3L",
          "productImages":{"altText":"Amazon Business Card"}
        }
      ]
    }
    </script>
    """

    products = scraper._collect_storefront_products_from_html(
        html_content,
        "https://www.amazon.com",
        set(),
    )

    assert products == [
        {
            "asin": "B0DKDT3X4Z",
            "link": "https://www.amazon.com/dp/B0DKDT3X4Z",
            "name": "VASAGLE MAEZO Collection Side Table",
        }
    ]
