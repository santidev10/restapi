from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_creation.models import *
from saas.utils_tests import ExtendedAPITestCase, SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
from django.conf import settings


class ItemsFromIdsAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.user.can_access_media_buying = True
        self.user.save()

    def test_success_video(self):
        if settings.DATABASES['default']['ENGINE'] != "django.db.backends.postgresql_psycopg2":
            logger.warning("This test requires postgres")
            return

        from segment.models import SegmentRelatedVideo, SegmentVideo
        ids = []
        item_ids = []
        video_ids = ["9bZkp7q19f0", "RgKAFK5djSk", "fRh_vgS2dFE", "OPf0YbXqDm0"]
        j = 0
        for i in range(2):
            segment = SegmentVideo.objects.create()
            for _ in range(2):
                item = SegmentRelatedVideo.objects.create(segment=segment, related_id=video_ids[j])
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
        self.assertEqual(set(response.data[0].keys()), {"id", "name", "thumbnail", "criteria"})

    def test_success_channel(self):
        if settings.DATABASES['default']['ENGINE'] != "django.db.backends.postgresql_psycopg2":
            logger.warning("This test requires postgres")
            return

        from segment.models import SegmentRelatedChannel, SegmentChannel
        ids = []
        item_ids = []
        channel_ids = ["UC-lHJZR3Gqxm24_Vd_AJ5Yw", "UCZJ7m7EnCNodqnu5SAtg8eQ",
                       "UCHkj014U2CQ2Nv0UZeYpE_A", "UCBR8-60-B28hp2BmDPdntcQ"]
        j = 0
        for i in range(2):
            segment = SegmentChannel.objects.create()
            for _ in range(2):
                item = SegmentRelatedChannel.objects.create(segment=segment, related_id=channel_ids[j])
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
        self.assertEqual(set(response.data[0].keys()), {"id", "name", "thumbnail", "criteria"})

    def test_success_keyword(self):
        if settings.DATABASES['default']['ENGINE'] != "django.db.backends.postgresql_psycopg2":
            logger.warning("This test requires postgres")
            return

        from keyword_tool.models import KeywordsList, KeyWord
        ids = []
        item_ids = []
        keywords = ["spam", "ham", "test", "batman"]
        j = 0
        for i in range(2):
            segment = KeywordsList.objects.create()
            for _ in range(2):
                item = KeyWord.objects.create(text=keywords[j])
                segment.keywords.add(item)
                item_ids.append(item.text)
                j += 1
            ids.append(segment.id)

        url = reverse("aw_creation_urls:items_from_segment_ids",
                      args=("keyword",))
        response = self.client.post(
            url, json.dumps(ids), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        self.assertEqual(set(response.data[0].keys()), {"name", "criteria"})
