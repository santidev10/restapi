from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from brand_safety.api.urls.names import BrandSafetyPathName
from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


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

    def test_duplicate_db_words(self):
        """ Test that all instances of a bad word are not returned if there are duplicates in db """
        word1 = word2 = "bad"
        category1 = "hit"
        category2 = "nonhit"

        badword = BadWord.objects.create(name=word1, category=BadWordCategory.objects.create(name=category1))
        BadWord.objects.create(name=word2, category=BadWordCategory.objects.create(name=category2))
        categories_data = {
            str(badword.category_id): {
                "keywords": [
                    {"keyword": badword.name}
                ]
            }
        }
        self.create_test_user()
        video = Video("test")
        video.populate_brand_safety(categories=categories_data)
        VideoManager(Sections.BRAND_SAFETY).upsert([video])
        url = reverse(
            BrandSafetyPathName.BrandSafety.GET_BRAND_SAFETY_VIDEO, [Namespace.BRAND_SAFETY],
            kwargs={"pk": video.main.id}
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        # There are two instances with the same word name. Only word1 should be returned as it is on the document
        self.assertTrue(category1 in data["category_flagged_words"])
        self.assertEqual(len(data["worst_words"]), 1)
        self.assertEqual(data["worst_words"][0], word1)
