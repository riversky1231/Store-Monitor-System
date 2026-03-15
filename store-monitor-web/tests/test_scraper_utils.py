import unittest

from scraper import _canonicalize_link


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


if __name__ == "__main__":
    unittest.main()
