from django.core.urlresolvers import reverse

from segment.models import SegmentChannel, SegmentKeyword, SegmentVideo
from utils.utils_tests import ExtendedAPITestCase


class SegmentDetailsApiViewTestCase(ExtendedAPITestCase):
    def test_segment_details_updated_at_field(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        segment_instances = [
            SegmentChannel.objects.create(),
            SegmentKeyword.objects.create(),
            SegmentVideo.objects.create()]
        urls = [
            reverse(
                "segment_api_urls:segment_details", kwargs={
                    "segment_type": segment.segment_type, "pk": segment.id})
            for segment in segment_instances]
        for url in urls:
            response = self.client.get(url)
            self.assertTrue("updated_at" in response.data.keys())
