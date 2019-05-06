from rest_framework.test import APIRequestFactory

from video.api.views import VideoListApiView
from utils.brand_safety_view_decorator import get_brand_safety_label
from utils.brand_safety_view_decorator import add_brand_safety_data
from utils.brand_safety_view_decorator import get_brand_safety_data
from utils.brand_safety_view_decorator import _handle_list_view
from utils.brand_safety_view_decorator import _handle_single_view
import brand_safety.constants as constants

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BrandSafetyDecoratorTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.request = APIRequestFactory()

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
        self.assertEqual(get_brand_safety_label(test_1["score"]), test_1["label"])
        self.assertEqual(get_brand_safety_label(test_2["score"]), test_2["label"])
        self.assertEqual(get_brand_safety_label(test_3["score"]), test_3["label"])
        self.assertEqual(get_brand_safety_label(test_4["score"]), test_4["label"])
        self.assertEqual(get_brand_safety_label(test_5["score"]), test_5["label"])
        self.assertEqual(get_brand_safety_label(test_6["score"]), test_6["label"])
        self.assertEqual(get_brand_safety_label(test_7["score"]), test_7["label"])
        self.assertEqual(get_brand_safety_label(test_8["score"]), test_8["label"])
        self.assertEqual(get_brand_safety_label(test_9["score"]), test_9["label"])

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
        self.assertEqual(get_brand_safety_data(test_1["score"]), test_1)
        self.assertEqual(get_brand_safety_data(test_2["score"]), test_2)
        self.assertEqual(get_brand_safety_data(test_3["score"]), test_3)
        self.assertEqual(get_brand_safety_data(test_4["score"]), test_4)
        self.assertEqual(get_brand_safety_data(test_5["score"]), test_5)
        self.assertEqual(get_brand_safety_data(test_6["score"]), test_6)
        self.assertEqual(get_brand_safety_data(test_7["score"]), test_7)
        self.assertEqual(get_brand_safety_data(test_8["score"]), test_8)
        self.assertEqual(get_brand_safety_data(test_9["score"]), test_9)

    def test_add_brand_safety_data_fail(self):
        decorated_view = add_brand_safety_data(VideoListApiView)
        response = decorated_view(VideoListApiView, self.request)
        print(response)
        print(response.__dict__)






