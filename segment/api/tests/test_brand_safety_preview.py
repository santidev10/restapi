from time import sleep

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
import uuid

from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.int_iterator import int_iterator


class SegmentPreviewApiViewTestCase(ExtendedAPITestCase):
    SECTIONS = [Sections.BRAND_SAFETY, Sections.SEGMENTS]

    def _get_url(self, segment_type, pk):
        return reverse(Namespace.SEGMENT + ":" + Name.PERSISTENT_SEGMENT_PREVIEW,
                       kwargs=dict(segment_type=segment_type, pk=pk))

    @staticmethod
    def get_mock_data(count, data_type, uuid):
        if data_type == "video":
            model = Video
        else:
            model = Channel
        data = []
        for i in range(count):
            item = {
                "meta": {
                    "id": data_type + str(i)
                },
                "brand_safety": {
                    "overall_score": 100 - i
                },
                "segments": {
                    "uuid": [uuid]
                }
            }
            if data_type == "channel":
                item["brand_safety"]["videos_scored"] = i
            data.append(model(**item))
        return data

    def test_persistent_segment_channel_segment_preview(self):
        self.create_admin_user()
        items = 5
        _id = next(int_iterator)
        segment = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(),
            title="test_title",
            id=_id,
        )
        mock_data = self.get_mock_data(5, "channel", str(segment.uuid))
        ChannelManager(sections=self.SECTIONS).upsert(mock_data)
        sleep(1)
        url = self._get_url("channel", segment.id) + "?page=1&size=5"
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), items)
        self.assertTrue(all(str(segment.uuid) in item["segments"]["uuid"] for item in response.data["items"]))
