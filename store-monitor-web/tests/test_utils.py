import os
import unittest

from utils import get_resource_path


class GetResourcePathTests(unittest.TestCase):
    def test_valid_relative_path_resolves(self):
        path = get_resource_path(os.path.join("static", "icon.png"))
        self.assertTrue(os.path.isabs(path))
        self.assertTrue(path.endswith(os.path.join("static", "icon.png")))

    def test_traversal_rejected(self):
        with self.assertRaises(ValueError):
            get_resource_path("..\\secret.txt")

    def test_absolute_path_rejected(self):
        with self.assertRaises(ValueError):
            get_resource_path(os.path.abspath("secret.txt"))

    def test_drive_path_rejected_on_windows(self):
        if os.name != "nt":
            self.skipTest("Windows-specific drive path test.")
        with self.assertRaises(ValueError):
            get_resource_path("C:\\Windows\\system32")


if __name__ == "__main__":
    unittest.main()
