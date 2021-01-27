from django.test import TransactionTestCase
from utils.utils import convert_subscriber_count


class SubscriberCountTestCase(TransactionTestCase):
    def test_subscriber_count_converter(self):
        subs_string_1 = "13.45k"
        subs_string_2 = "839"
        subs_string_3 = 0
        subs_string_4 = "122.4M"
        subs_string_5 = "1.2b"
        subs_string_6 = "98m"
        subs_string_7 = "13.4k"
        subs_string_8 = "13.4m"
        subs_string_9 = "13.4b"
        subs_string_10 = None
        self.assertEqual(convert_subscriber_count(subs_string_1), 13450)
        self.assertEqual(convert_subscriber_count(subs_string_2), 839)
        self.assertEqual(convert_subscriber_count(subs_string_3), 0)
        self.assertEqual(convert_subscriber_count(subs_string_4), 122400000)
        self.assertEqual(convert_subscriber_count(subs_string_5), 1200000000)
        self.assertEqual(convert_subscriber_count(subs_string_6), 98000000)
        self.assertEqual(convert_subscriber_count(subs_string_7), 13400)
        self.assertEqual(convert_subscriber_count(subs_string_8), 13400000)
        self.assertEqual(convert_subscriber_count(subs_string_9), 13400000000)
        self.assertEqual(convert_subscriber_count(subs_string_10), None)
