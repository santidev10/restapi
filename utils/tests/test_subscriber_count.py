from django.test import TestCase
from utils.utils import convert_subscriber_count


class SubscriberCountTestCase(TestCase):
    def test_subscriber_count_converter(self):
        subs_string_1 = "13.45k"
        subs_string_2 = "839"
        subs_string_3 = 0
        subs_string_4 = "122.4M"
        subs_string_5 = "1.2b"
        self.assertEqual(convert_subscriber_count(subs_string_1), 13450)
        self.assertEqual(convert_subscriber_count(subs_string_2), 839)
        self.assertEqual(convert_subscriber_count(subs_string_3), 0)
        self.assertEqual(convert_subscriber_count(subs_string_4), 122400000)
        self.assertEqual(convert_subscriber_count(subs_string_5), 1200000000)
