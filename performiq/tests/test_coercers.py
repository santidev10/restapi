from utils.unittests.test_case import ExtendedAPITestCase

from performiq.analyzers.utils import Coercers


class CoercerTestCase(ExtendedAPITestCase):
    def test_invalid_values(self):
        """ Test that coercers do not raise on invalid values"""
        invalid = (None, "--", " ")

        for val in invalid:
            for func in (Coercers.percentage, Coercers.integer, Coercers.float, Coercers.cost_micros):
                with self.subTest(f"Test: {func.__name__} with {val}"):
                    result = func(val)
                    self.assertEqual(result, None)

    def test_percentage(self):
        result = Coercers.percentage("%1.22")
        self.assertEqual(result, 1.22)

        result = Coercers.percentage(0.5)
        self.assertEqual(result, 0.5)

        result = Coercers.percentage("0.77")
        self.assertEqual(result, 0.77)