from django.test import TestCase

from utils.datetime import seconds_to_hhmmss


class TestSecondsToHHMMSSTestCase(TestCase):
    """
    tests that seconds are always converted to a HHH:MM:SS format
    """
    @staticmethod
    def get_seconds_to_test():
        """
        return a list of seconds that will produce different
        numbers of hours, minutes, and seconds
        """
        return [12, 234, 4335, 93442]

    def test_formatted_is_a_string(self):
        for seconds in self.get_seconds_to_test():
            with self.subTest(seconds):
                formatted = seconds_to_hhmmss(seconds)
                self.assertIsInstance(formatted, str)

    def test_three_parts_always_present(self):
        """
        ensure HH:MM:SS even if hours or minutes is zero
        """
        for seconds in self.get_seconds_to_test():
            with self.subTest(seconds):
                formatted = seconds_to_hhmmss(seconds)
                split = formatted.split(":")
                self.assertEqual(len(split), 3)

    def test_part_length_is_at_least_two(self):
        """
        we don't want HH:M:SS, or H:M:S
        """
        for seconds in [15, 60, 66]:
            with self.subTest(seconds):
                formatted = seconds_to_hhmmss(seconds)
                split = formatted.split(":")
                for part in split:
                    with self.subTest(part):
                        self.assertIsInstance(part, str)
                        self.assertGreaterEqual(len(part), 2)

    def test_over_two_digit_hours(self):
        """
        we want HHH:MM:SS instead of DD:HH:MM:SS
        """
        seconds = 123345
        formatted = seconds_to_hhmmss(seconds)
        hours, minutes, seconds = formatted.split(":")
        self.assertIsInstance(hours, str)
        self.assertGreaterEqual(len(hours), 2)