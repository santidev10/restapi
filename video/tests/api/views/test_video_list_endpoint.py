from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND

from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.segment_functionality_mixin import SegmentFunctionalityMixin


class VideoListTestCase(ExtendedAPITestCase, SegmentFunctionalityMixin):
    def setUp(self):
        self.url = reverse("video_api_urls:video_list")

    def test_simple_list_works(self):
        self.create_admin_user()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
