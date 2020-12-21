from django.test import TestCase

import brand_safety.constants as constants
from utils.brand_safety import get_brand_safety_data
from utils.brand_safety import get_brand_safety_label


class BrandSafetyDecoratorTestCase(TestCase):
    def test_get_brand_safety_label(self):
        test_1 = {
            "score": 90,
            "label": constants.SAFE
        }
        test_2 = {
            "score": 80,
            "label": constants.LOW_RISK
        }
        test_3 = {
            "score": 70,
            "label": constants.RISKY
        }
        test_4 = {
            "score": 60,
            "label": constants.HIGH_RISK
        }
        test_5 = {
            "score": 1000,
            "label": constants.SAFE
        }
        test_6 = {
            "score": "a",
            "label": None
        }
        test_7 = {
            "score": "1b",
            "label": None
        }
        test_8 = {
            "score": "!",
            "label": None
        }
        test_9 = {
            "score": "5P",
            "label": None
        }
        self.assertEqual(get_brand_safety_label(test_1["score"])[0], test_1["label"])
        self.assertEqual(get_brand_safety_label(test_2["score"])[0], test_2["label"])
        self.assertEqual(get_brand_safety_label(test_3["score"])[0], test_3["label"])
        self.assertEqual(get_brand_safety_label(test_4["score"])[0], test_4["label"])
        self.assertEqual(get_brand_safety_label(test_5["score"])[0], test_5["label"])
        self.assertEqual(get_brand_safety_label(test_6["score"])[0], test_6["label"])
        self.assertEqual(get_brand_safety_label(test_7["score"])[0], test_7["label"])
        self.assertEqual(get_brand_safety_label(test_8["score"])[0], test_8["label"])
        self.assertEqual(get_brand_safety_label(test_9["score"])[0], test_9["label"])

    def test_get_brand_safety_data(self):
        test_1 = {
            "score": 90,
            "label": constants.SAFE
        }
        test_2 = {
            "score": 80,
            "label": constants.LOW_RISK
        }
        test_3 = {
            "score": 70,
            "label": constants.RISKY
        }
        test_4 = {
            "score": 60,
            "label": constants.HIGH_RISK
        }
        test_5 = {
            "score": 100,
            "label": constants.SAFE
        }
        test_6 = {
            "score": "a",
            "label": None
        }
        test_7 = {
            "score": "1b",
            "label": None
        }
        test_8 = {
            "score": "!",
            "label": None
        }
        test_9 = {
            "score": "5P",
            "label": None
        }
        self.assertEqual(get_brand_safety_data(test_1["score"]), {"score": 9, "label": test_1["label"]})
        self.assertEqual(get_brand_safety_data(test_2["score"]), {"score": 8, "label": test_2["label"]})
        self.assertEqual(get_brand_safety_data(test_3["score"]), {"score": 7, "label": test_3["label"]})
        self.assertEqual(get_brand_safety_data(test_4["score"]), {"score": 6, "label": test_4["label"]})
        self.assertEqual(get_brand_safety_data(test_5["score"]), {"score": 10, "label": test_5["label"]})
        self.assertEqual(get_brand_safety_data(test_6["score"]), test_6)
        self.assertEqual(get_brand_safety_data(test_7["score"]), test_7)
        self.assertEqual(get_brand_safety_data(test_8["score"]), test_8)
        self.assertEqual(get_brand_safety_data(test_9["score"]), test_9)
