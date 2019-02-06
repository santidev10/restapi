import json
import logging
from unittest.mock import patch

from django.conf import settings
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher
from utils.utittests.test_case import ExtendedAPITestCase

logger = logging.getLogger(__name__)


class ItemsFromIdsAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_media_buying")

    def test_success_video(self):
        if settings.DATABASES['default'][
            'ENGINE'] != "django.db.backends.postgresql_psycopg2":
            logger.warning("This test requires postgres")
            return

        from segment.models import SegmentRelatedVideo, SegmentVideo
        ids = []
        item_ids = []
        video_ids = ["aSjflT_J0Xo", "xp6706wVdCI", "fRh_vgS2dFE", "gFZfwWZV074"]
        j = 0
        for i in range(2):
            segment = SegmentVideo.objects.create()
            for _ in range(2):
                item = SegmentRelatedVideo.objects.create(segment=segment,
                                                          related_id=video_ids[
                                                              j])
                item_ids.append(item.related_id)
                j += 1
            ids.append(segment.id)

        url = reverse("aw_creation_urls:items_from_segment_ids",
                      args=("video",))
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(ids), content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        self.assertEqual(set(response.data[0].keys()),
                         {"id", "name", "thumbnail", "criteria"})

    def test_success_channel(self):
        if settings.DATABASES['default'][
            'ENGINE'] != "django.db.backends.postgresql_psycopg2":
            logger.warning("This test requires postgres")
            return

        from segment.models import SegmentRelatedChannel, SegmentChannel
        ids = []
        item_ids = []
        channel_ids = ["UCF0OLOFwYYu5SkDNunKMbqw", "UCZJ7m7EnCNodqnu5SAtg8eQ",
                       "UCHkj014U2CQ2Nv0UZeYpE_A", "UCLGOpkME9pyo3gDzWWrJwqA"]
        j = 0
        for i in range(2):
            segment = SegmentChannel.objects.create()
            for _ in range(2):
                item = SegmentRelatedChannel.objects.create(segment=segment,
                                                            related_id=
                                                            channel_ids[j])
                item_ids.append(item.related_id)
                j += 1
            ids.append(segment.id)

        url = reverse("aw_creation_urls:items_from_segment_ids",
                      args=("channel",))
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(ids), content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        self.assertEqual(set(response.data[0].keys()),
                         {"id", "name", "thumbnail", "criteria"})

    def test_success_keyword(self):
        if settings.DATABASES['default'][
            'ENGINE'] != "django.db.backends.postgresql_psycopg2":
            logger.warning("This test requires postgres")
            return
        from segment.models import SegmentKeyword, SegmentRelatedKeyword
        ids = []
        item_ids = []
        keywords = ["spam", "ham", "test", "batman"]
        j = 0
        for i in range(2):
            segment = SegmentKeyword.objects.create()
            for _ in range(2):
                item = SegmentRelatedKeyword.objects.create(segment=segment,
                                                            related_id=keywords[
                                                                j])
                item_ids.append(item.related_id)
                j += 1
            ids.append(segment.id)

        url = reverse("aw_creation_urls:items_from_segment_ids",
                      args=("keyword",))
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(ids), content_type='application/json',
            )

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        self.assertEqual(set(response.data[0].keys()), {"name", "criteria"})
