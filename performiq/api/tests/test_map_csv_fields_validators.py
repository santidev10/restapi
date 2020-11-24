from django.test import testcases
from django.core.exceptions import ValidationError
from performiq.utils.map_csv_fields import is_cpm_validator
from performiq.utils.map_csv_fields import is_cpv_validator
from performiq.utils.map_csv_fields import is_currency_validator
from performiq.utils.map_csv_fields import is_integer_validator
from performiq.utils.map_csv_fields import is_rate_validator


class MapCSVFieldsValidatorTestCase(testcases.TestCase):

    def test_invalid_cpm(self):
        for invalid in [0.5, ".123", 1123, 10, [], "asdf", {}]:
            with self.subTest(invalid):
                with self.assertRaises(ValidationError):
                    is_cpm_validator(invalid)

    def test_valid_cpm(self):
        for valid in ["4.11", 3.43, 1.17]:
            with self.subTest(valid):
                is_cpm_validator(valid)

    def test_invalid_cpv(self):
        for invalid in [1, "1", 2.1, 0.2, [], "asdf", {}]:
            with self.subTest(invalid):
                with self.assertRaises(ValidationError):
                    is_cpv_validator(invalid)

    def test_valid_cpv(self):
        for valid in [0.01, "0.01", "0.09", 0.02]:
            with self.subTest(valid):
                is_cpv_validator(valid)

    def test_invalid_currency(self):
        for invalid in [14, "a", "one", "dollars"]:
            with self.subTest(invalid):
                with self.assertRaises(ValidationError):
                    is_currency_validator(invalid)

    def test_valid_currency(self):
        for valid in ["$1", "USD", 11.17, "15 SEK"]:
            with self.subTest(valid):
                is_currency_validator(valid)

    def test_invalid_integer(self):
        for invalid in ["asdf", "one", 11.1, ""]:
            with self.subTest(invalid):
                with self.assertRaises(ValidationError):
                    is_integer_validator(invalid)

    def test_valid_integer(self):
        for valid in ["1", "1.0", 117, 1.000]:
            with self.subTest(valid):
                is_integer_validator(valid)

    def test_invalid_rate(self):
        for invalid in ["asdf", "one", "117", 117, 343.43, -0.0001, 100.0001]:
            with self.subTest(invalid):
                with self.assertRaises(ValidationError):
                    is_rate_validator(invalid)

    def test_valid_rate(self):
        for valid in ["11.7", "12", 100, 11.7, 3.43, 0, "0"]:
            with self.subTest(valid):
                is_rate_validator(valid)