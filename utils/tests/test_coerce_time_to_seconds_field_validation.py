from django.test import TransactionTestCase
from rest_framework.exceptions import ValidationError
from rest_framework.serializers import empty

from utils.serializers.fields.coerce_time_to_seconds_field import CoerceTimeToSecondsField


class CoerceTimeToSecondsFieldValidationTestCase(TransactionTestCase):

    @staticmethod
    def get_field_instance(**kwargs):
        return CoerceTimeToSecondsField(**kwargs)

    def test_null_validation(self):
        for data in [None, ""]:
            with self.subTest(data):
                instance = self.get_field_instance(allow_null=True)
                validated = instance.run_validation(data=data)
                self.assertEqual(validated, None)

                with self.assertRaises(ValidationError):
                    instance = self.get_field_instance(allow_null=False)
                    instance.run_validation(data=data)

    def test_empty_validation(self):
        instance = self.get_field_instance(required=True)
        with self.assertRaises(ValidationError):
            instance.run_validation(data=empty)

        instance = self.get_field_instance(required=False)
        validated = instance.run_validation(data=empty)
        self.assertEqual(validated, None)

    def test_integers_that_integers_are_allowed(self):
        instance = self.get_field_instance(required=True)
        data = 1337
        validated = instance.run_validation(data=data)
        self.assertIsInstance(validated, int)
        self.assertEqual(validated, data)

    def test_string_integer_coercion(self):
        instance = self.get_field_instance(required=True)
        data = "1337"
        validated = instance.run_validation(data=data)
        self.assertIsInstance(validated, int)
        self.assertEqual(validated, int(data))

    def test_invalid_string_format(self):
        instance = self.get_field_instance(required=True)
        for data in ["1 day, 0:00:00", "1:2:3:4", "an invalid string", "five", "fifteen"]:
            with self.assertRaises(ValidationError):
                instance.run_validation(data=data)

    def test_ceiling_enforced(self):
        instance = self.get_field_instance(required=True)
        for data in ["123:30", "48:61"]:
            with self.subTest(data):
                with self.assertRaises(ValidationError):
                    instance.run_validation(data=data)

    def test_conversion_to_seconds(self):
        instance = self.get_field_instance(required=True)
        for data, expected_seconds in {
            "33:14": 1994,
            "2:01:54": 7314,
        }.items():
            with self.subTest(data=data, expected_seconds=expected_seconds):
                validated = instance.run_validation(data=data)
                self.assertEqual(validated, expected_seconds)