import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from scraper import (
    _attach_traffic_listeners,
    _browser_user_agent,
    _canonicalize_link,
    _discover_storefront_tabs_from_html,
    _extract_next_page_url,
    _install_network_request_guard,
    _navigate_with_retry,
    _normalize_delivery_text,
    _normalize_storefront_tab_url,
    _playwright_launch_env,
    _preferred_us_zip,
    _reload_after_delivery_update,
    _request_amazon_delivery_change,
    _response_download_size,
    _storage_state_path,
    _title_from_amazon_product_link,
)
from traffic_metrics import TrafficCapture


class CanonicalizeLinkTests(unittest.TestCase):
    def test_canonicalize_amazon_product_links_to_dp(self):
        base_url = "https://www.amazon.com/s?k=headphones"
        raw_link = "/gp/product/b0abc12345?ref_=abc"
        self.assertEqual(
            _canonicalize_link(base_url, raw_link),
            "https://www.amazon.com/dp/B0ABC12345",
        )

    def test_canonicalize_relative_link_without_trailing_slash(self):
        base_url = "https://example.com/store"
        raw_link = "/items/sku-1/"
        self.assertEqual(
            _canonicalize_link(base_url, raw_link),
            "https://example.com/items/sku-1",
        )

    def test_canonicalize_empty_link(self):
        self.assertEqual(_canonicalize_link("https://example.com", ""), "")

    def test_browser_user_agent_uses_detected_browser_version(self):
        class DummyBrowser:
            version = "137.0.7151.15"

        user_agent = _browser_user_agent(DummyBrowser())
        self.assertIn("Chrome/137.0.7151.15", user_agent)

    def test_storage_state_path_is_deterministic(self):
        first = _storage_state_path("https://www.amazon.com/s?me=A123456")
        second = _storage_state_path("https://www.amazon.com/s?me=A123456")
        self.assertEqual(first, second)
        self.assertTrue(first.name.startswith("www.amazon.com-"))
        self.assertEqual(first.suffix, ".json")

    def test_extract_next_page_url_prefers_dom_link(self):
        class DummyNode:
            def __init__(self, href):
                self._href = href

            def get_attribute(self, name):
                if name == "href":
                    return self._href
                return None

        class DummyLocator:
            def __init__(self, hrefs):
                self._hrefs = hrefs

            def count(self):
                return len(self._hrefs)

            def nth(self, index):
                return DummyNode(self._hrefs[index])

        class DummyPage:
            def __init__(self):
                self._selectors = {
                    "a.s-pagination-next[href]": DummyLocator(["/s?me=A123456&page=2"]),
                }

            def locator(self, selector):
                return self._selectors.get(selector, DummyLocator([]))

        next_url = _extract_next_page_url(
            DummyPage(),
            "https://www.amazon.com",
            "https://www.amazon.com/s?me=A123456",
            1,
        )
        self.assertEqual(next_url, "https://www.amazon.com/s?me=A123456&page=2")

    def test_playwright_launch_env_uses_user_override_path(self):
        with TemporaryDirectory() as tempdir:
            first = Path(tempdir) / "lib1"
            second = Path(tempdir) / "lib2"
            first.mkdir()
            second.mkdir()
            with patch.dict(
                "os.environ",
                {
                    "MONITOR_WEB_PLAYWRIGHT_LD_LIBRARY_PATH": f"{first}:{second}",
                    "LD_LIBRARY_PATH": "/existing/lib",
                },
                clear=False,
            ):
                launch_env = _playwright_launch_env()
        self.assertIsNotNone(launch_env)
        library_path = launch_env["LD_LIBRARY_PATH"]
        self.assertTrue(library_path.startswith(f"{first}:{second}"))
        self.assertIn("/existing/lib", library_path)

    def test_preferred_us_zip_normalizes_env_value(self):
        with patch.dict("os.environ", {"MONITOR_WEB_US_ZIP": "10001-1234"}, clear=False):
            self.assertEqual(_preferred_us_zip(), "10001")
        with patch.dict("os.environ", {"MONITOR_WEB_US_ZIP": "abc"}, clear=False):
            self.assertEqual(_preferred_us_zip(), "10001")

    def test_normalize_delivery_text_removes_zero_width_noise(self):
        self.assertEqual(
            _normalize_delivery_text("New York 10001\u200c"),
            "new york 10001",
        )

    def test_title_from_amazon_product_link_uses_slug(self):
        title = _title_from_amazon_product_link(
            "https://www.amazon.com/VASAGLE-Square-Nightstand-Bedroom-ULET236K01/dp/B0DKDT3X4Z?ref_=ast_sto_dp"
        )
        self.assertEqual(title, "VASAGLE Square Nightstand Bedroom ULET236K01")

    def test_normalize_storefront_tab_url_drops_tracking_params(self):
        normalized = _normalize_storefront_tab_url(
            "https://www.amazon.com/stores/page/DCE9F236-97E8-466A-82F5-167CE44C3FFE?ingress=2&visitId=abc&ref_=ast_bln&pageId=1"
        )
        self.assertEqual(
            normalized,
            "https://www.amazon.com/stores/page/DCE9F236-97E8-466A-82F5-167CE44C3FFE?pageId=1",
        )

    def test_discover_storefront_tabs_from_html_uses_hidden_links(self):
        html_content = """
        <script type="application/json">
        {
          "tabs": [
            {"url":"\\/stores\\/page\\/DCE9F236-97E8-466A-82F5-167CE44C3FFE?ingress=2&visitId=abc"},
            {"url":"\\/stores\\/page\\/CFE984B3-E195-4D09-832F-750DBE77FCBF?ref_=ast_bln"}
          ]
        }
        </script>
        """

        tabs = _discover_storefront_tabs_from_html(html_content, "https://www.amazon.com")

        self.assertEqual(
            tabs,
            [
                "https://www.amazon.com/stores/page/DCE9F236-97E8-466A-82F5-167CE44C3FFE?ingress=2&visitId=abc",
                "https://www.amazon.com/stores/page/CFE984B3-E195-4D09-832F-750DBE77FCBF?ref_=ast_bln",
            ],
        )


class RequestAmazonDeliveryChangeTests(unittest.TestCase):
    """测试 _request_amazon_delivery_change 函数（无需真实浏览器）。"""

    def _make_page(self, evaluate_return=None, evaluate_raises=None):
        page = MagicMock()
        if evaluate_raises:
            page.evaluate.side_effect = evaluate_raises
        else:
            page.evaluate.return_value = evaluate_return
        return page

    def test_returns_response_on_success(self):
        expected = {"ok": True, "status": 200, "data": {"successful": True, "isValidAddress": True}, "raw": ""}
        page = self._make_page(evaluate_return=expected)
        result = _request_amazon_delivery_change(page, token="tok123", desired_zip="10001")
        self.assertEqual(result, expected)
        page.evaluate.assert_called_once()

    def test_returns_none_on_exception(self):
        page = self._make_page(evaluate_raises=Exception("network error"))
        result = _request_amazon_delivery_change(page, token="tok123", desired_zip="10001")
        self.assertIsNone(result)

    def test_passes_token_and_zip_to_evaluate(self):
        page = self._make_page(evaluate_return={"ok": True, "status": 200, "data": {}, "raw": ""})
        _request_amazon_delivery_change(page, token="MY_TOKEN", desired_zip="90210")
        _, kwargs_or_args = page.evaluate.call_args_list[0][0], page.evaluate.call_args_list[0][1]
        call_args = page.evaluate.call_args
        self.assertEqual(call_args[0][1], {"token": "MY_TOKEN", "zipCode": "90210"})


class ReloadAfterDeliveryUpdateTests(unittest.TestCase):
    """测试 _reload_after_delivery_update 函数（无需真实浏览器）。"""

    def _make_page(self, reload_raises=None, location_text="New York 10001"):
        page = MagicMock()
        if reload_raises:
            page.reload.side_effect = reload_raises
        page.url = "https://www.amazon.com/s?me=A123"

        # Mock _amazon_delivery_location via the page's inner_text calls
        return page

    def test_calls_reload_on_success(self):
        page = self._make_page()
        with patch("scraper._amazon_delivery_location", return_value="New York 10001"), \
             patch("scraper._dismiss_common_overlays"), \
             patch("scraper._update_activity"):
            _reload_after_delivery_update(page, "https://www.amazon.com/s?me=A123", "10001")
        page.reload.assert_called_once()

    def test_falls_back_to_navigate_on_reload_failure(self):
        page = self._make_page(reload_raises=Exception("reload failed"))
        with patch("scraper._amazon_delivery_location", return_value=""), \
             patch("scraper._dismiss_common_overlays"), \
             patch("scraper._update_activity"), \
             patch("scraper._navigate_with_retry", return_value=True) as mock_nav:
            _reload_after_delivery_update(page, "https://www.amazon.com/s?me=A123", "10001")
        mock_nav.assert_called_once()


class NavigateWithRetryTests(unittest.TestCase):
    def test_rejects_unsafe_target_before_navigation(self):
        page = MagicMock()

        with patch("scraper._check_cancel"), \
             patch("scraper.time.sleep"), \
             patch("scraper.validate_monitor_target_url", side_effect=ValueError("private host")):
            result = _navigate_with_retry(page, "http://127.0.0.1/internal", max_attempts=1)

        self.assertFalse(result)
        page.goto.assert_not_called()

    def test_rejects_redirect_to_unsafe_final_url(self):
        page = MagicMock()
        page.url = "http://127.0.0.1/internal"

        with patch("scraper._check_cancel"), \
             patch("scraper.time.sleep"), \
             patch(
                 "scraper.validate_monitor_target_url",
                 side_effect=["https://www.amazon.com/s?k=test", ValueError("private host")],
             ):
            result = _navigate_with_retry(page, "https://www.amazon.com/s?k=test", max_attempts=1)

        self.assertFalse(result)
        page.goto.assert_called_once()

    def test_accepts_public_final_url(self):
        page = MagicMock()
        page.url = "https://www.amazon.com/s?k=test"

        with patch("scraper._check_cancel"), \
             patch("scraper.time.sleep"), \
             patch("scraper._update_activity") as update_mock, \
             patch("scraper.validate_monitor_target_url", return_value=page.url):
            result = _navigate_with_retry(page, "https://www.amazon.com/s?k=test", max_attempts=1)

        self.assertTrue(result)
        update_mock.assert_called_once()


class NetworkRequestGuardTests(unittest.TestCase):
    def _handler(self):
        context = MagicMock()
        _install_network_request_guard(context)
        context.route.assert_called_once_with("**/*", unittest.mock.ANY)
        return context.route.call_args.args[1]

    def test_private_redirect_request_is_aborted_before_continue(self):
        handler = self._handler()
        route = MagicMock()
        route.request.url = "http://127.0.0.1/admin"
        route.request.resource_type = "document"
        route.request.is_navigation_request.return_value = True

        with patch("scraper.validate_monitor_target_url", side_effect=ValueError("private")) as validate:
            handler(route)

        validate.assert_called_once_with(
            "http://127.0.0.1/admin",
            force_dns_refresh=True,
        )
        route.abort.assert_called_once_with("blockedbyclient")
        route.continue_.assert_not_called()

    def test_public_subresource_is_continued(self):
        handler = self._handler()
        route = MagicMock()
        route.request.url = "https://images.example.com/product.jpg"
        route.request.resource_type = "image"
        route.request.is_navigation_request.return_value = False

        with patch("scraper.validate_monitor_target_url", return_value=route.request.url) as validate:
            handler(route)

        validate.assert_called_once_with(route.request.url, force_dns_refresh=False)
        route.continue_.assert_called_once_with()
        route.abort.assert_not_called()


class TrafficMetricsHookTests(unittest.TestCase):
    def test_response_download_size_prefers_content_length(self):
        response = MagicMock()
        response.headers = {"content-length": "2048"}
        response.request = MagicMock(resource_type="image")

        size, measured = _response_download_size(response)

        self.assertEqual(size, 2048)
        self.assertTrue(measured)
        response.body.assert_not_called()

    def test_response_download_size_falls_back_to_body_for_text(self):
        request = MagicMock()
        request.resource_type = "document"
        response = MagicMock()
        response.headers = {"content-type": "text/html; charset=utf-8"}
        response.request = request
        response.body.return_value = b"<html>hello</html>"

        size, measured = _response_download_size(response)

        self.assertEqual(size, len(b"<html>hello</html>"))
        self.assertTrue(measured)
        response.body.assert_called_once()

    def test_attach_traffic_listeners_records_request_and_response_bytes(self):
        handlers = {}
        capture = TrafficCapture(task_id=1, task_name="Task", task_url="https://example.com")

        class DummyPage:
            def on(self, event_name, handler):
                handlers[event_name] = handler

        request = MagicMock()
        request.resource_type = "xhr"
        request.post_data_buffer = b"payload"

        response = MagicMock()
        response.headers = {"content-length": "1024"}
        response.request = request

        with patch("scraper._current_traffic_capture", return_value=capture):
            _attach_traffic_listeners(DummyPage())

        handlers["request"](request)
        handlers["response"](response)
        snapshot = capture.snapshot()

        self.assertEqual(snapshot["request_count"], 1)
        self.assertEqual(snapshot["upload_bytes"], len(b"payload"))
        self.assertEqual(snapshot["download_bytes"], 1024)
        self.assertEqual(snapshot["resource_breakdown"][0]["type"], "xhr")


if __name__ == "__main__":
    unittest.main()
