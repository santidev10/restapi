from django.test.testcases import TestCase

from rest_framework.exceptions import ValidationError

from segment.api.serializers.ctl_params_serializer import AdsPerformanceRangeField


class TestAdsPerformanceRangeField(TestCase):

    @staticmethod
    def get_field_instance(**kwargs):
        return AdsPerformanceRangeField(**kwargs)

    def test_accepted_values(self):
        instance = self.get_field_instance()
        for data in ["1,14", "12, ", ", 14", "0.14,", "0.34, 1.16"]:
            with self.subTest(data):
                validated = instance.run_validation(data=data)
                self.assertEqual(data, validated)

    def test_formatting_is_enforced(self):
        instance = self.get_field_instance()
        for data in ["asdf", "1,2,3", "so not,valid", "invalid,", ",invalid"]:
            with self.subTest(data):
                with self.assertRaises(ValidationError):
                    instance.run_validation(data=data)

    def test_valid_range_is_enforced(self):
        instance = self.get_field_instance()
        for data in ["4,2", "3.14, 1.17"]:
            with self.subTest(data):
                with self.assertRaises(ValidationError):
                    instance.run_validation(data=data)

    def test_null_or_null_like_values_are_coerced_to_none(self):
        instance = self.get_field_instance()
        for data in [",", ", ", None]:
            with self.subTest(data):
                validated = instance.run_validation(data=data)
                self.assertEqual(validated, None)