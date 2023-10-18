import unittest
from app import main


class MainCase(unittest.TestCase):

    def test_app_test(self):
        result = main.app_test(True)
        self.assertTrue(result)
