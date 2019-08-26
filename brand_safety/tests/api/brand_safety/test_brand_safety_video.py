from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from brand_safety.api.urls.names import BrandSafetyPathName
from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class BrandSafetyVideoApiViewTestCase(ExtendedAPITestCase, ESTestCase):
    def test_brand_safety_video(self):
        self.create_test_user()
        video = Video("test")
        VideoManager(Sections.BRAND_SAFETY).upsert([video])
        url = reverse(
            BrandSafetyPathName.BrandSafety.GET_BRAND_SAFETY_VIDEO, [Namespace.BRAND_SAFETY],
            kwargs={"pk": video.main.id}
        )
        response = self.client.get(url)
        response_keys = {
            "score",
            "label",
            "total_unique_flagged_words",
            "category_flagged_words",
            "worst_words"
        }
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()), response_keys)

    def test_brand_safety_video_not_found(self):
        self.create_test_user()
        url = reverse(
            BrandSafetyPathName.BrandSafety.GET_BRAND_SAFETY_VIDEO, [Namespace.BRAND_SAFETY],
            kwargs={"pk": "test"}
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
