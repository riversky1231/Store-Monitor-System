"""Unit tests for scraper._canonicalize_link and scraper._sync_products_to_db."""
import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import scraper
from database import Base
from models import MonitorTask, ProductItem
from scraper import ScrapeIncomplete, _canonicalize_link, _sync_products_to_db


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
