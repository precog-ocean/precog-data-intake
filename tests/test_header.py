import unittest
from scripts.intake_UtilFuncs import *

class TestPrint(unittest.TestCase):
    def test_precog_print_header(self):
        self.assertEqual(print_precog_header(), None)

if __name__ == "__main__":
    # Run the tests
    result = unittest.main(exit=False)

    # If all tests passed, print the message
    if result.result.wasSuccessful():
        print("Passed")