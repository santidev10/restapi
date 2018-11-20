from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_201_CREATED

from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import SegmentChannel
from utils.utittests.test_case import ExtendedAPITestCase


class SegmentDuplicateTestCase(ExtendedAPITestCase):
    def _get_url(self, segment_type, segment_id):
        return reverse(Namespace.SEGMENT + ":" + Name.SEGMENT_DUPLICATE, args=(segment_type, segment_id))

    def test_success(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_pre_baked_segments")
        segment = SegmentChannel.objects.create(title="Test name", likes=123)
        self.assertEqual(SegmentChannel.objects.count(), 1)
        url = self._get_url("channel", segment.id)
        response = self.client.post(url)

        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(SegmentChannel.objects.count(), 2)
        new_segment = SegmentChannel.objects.exclude(id=segment.id).first()
        self.assertEqual(new_segment.title, "{} (copy)".format(segment.title))
        self.assertEqual(new_segment.likes, segment.likes)
