from mock import patch

import boto3
from django.conf import settings
from django.contrib.auth import get_user_model
from moto import mock_s3

from audit_tool.models import get_hash_name
from audit_tool.models import BlacklistItem
from audit_tool.api.serializers.blocklist_serializer import BlocklistSerializer
from audit_tool.tasks.export_blocklist import export_blocklist_task
from es_components.models import Channel
from es_components.models import Video
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.tests.utils import ESTestCase
from es_components.constants import Sections
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class BlocklistExportTaskTestCase(ExtendedAPITestCase, ESTestCase):
    multi_db = True
    SECTIONS = (Sections.CUSTOM_PROPERTIES, Sections.BRAND_SAFETY, Sections.GENERAL_DATA)
    channel_manager = ChannelManager(SECTIONS)
    video_manager = VideoManager(SECTIONS)

    @mock_s3
    def test_channel_blocklist_export(self):
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_BUCKET_NAME)
        user = get_user_model().objects.create(id=next(int_iterator), email=f"test{next(int_iterator)}@test.com")

        blocklist_item = Channel(f"youtube_channel_id_{next(int_iterator)}")
        blocklist_item.populate_custom_properties(blocklist=True)
        blocklist_item.populate_general_data(title=f"Test title {blocklist_item.main.id}")
        bl_data = BlacklistItem.objects.create(
            processed_by_user_id=user.id, blocked_count=3, unblocked_count=2, item_type=1,
            item_id=blocklist_item.main.id, item_id_hash=get_hash_name(blocklist_item.main.id),
        )
        non_blocklist = Channel(next(int_iterator))
        self.channel_manager.upsert([blocklist_item, non_blocklist])

        mock_export_key = "export.csv"
        with patch("audit_tool.tasks.export_blocklist._get_export_key", return_value=mock_export_key):
            export_blocklist_task.delay(user.email, "channel")

        body = conn.Object(settings.AMAZON_S3_BUCKET_NAME, mock_export_key).get()["Body"]
        data = [row.decode("utf-8").strip().split("\n") for row in body][0]
        rows = [row.strip().split(",") for row in data]
        self.assertEqual(len(rows), 2)
        self.assertEqual(set(rows[0]), set(BlocklistSerializer.EXPORT_FIELDS))
        data = rows[1]
        expected = [blocklist_item.general_data.title,f"https://www.youtube.com/channel/{blocklist_item.main.id}",
                    str(bl_data.updated_at), user.email, str(bl_data.blocked_count), str(bl_data.unblocked_count)]
        self.assertEqual(data, expected)

    @mock_s3
    def test_video_blocklist_export(self):
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_BUCKET_NAME)
        user = get_user_model().objects.create(id=next(int_iterator), email=f"test{next(int_iterator)}@test.com")

        blocklist_item = Video(f"video{next(int_iterator)}")
        blocklist_item.populate_custom_properties(blocklist=True)
        blocklist_item.populate_general_data(title=f"Test title {blocklist_item.main.id}")
        bl_data = BlacklistItem.objects.create(
            processed_by_user_id=user.id, blocked_count=3, unblocked_count=2, item_type=0,
            item_id=blocklist_item.main.id, item_id_hash=get_hash_name(blocklist_item.main.id),
        )
        non_blocklist = Video(next(int_iterator))
        self.video_manager.upsert([blocklist_item, non_blocklist])

        mock_export_key = "export.csv"
        with patch("audit_tool.tasks.export_blocklist._get_export_key", return_value=mock_export_key):
            export_blocklist_task.delay(user.email, "channel")

        body = conn.Object(settings.AMAZON_S3_BUCKET_NAME, mock_export_key).get()["Body"]
        data = [row.decode("utf-8").strip().split("\n") for row in body][0]
        rows = [row.strip().split(",") for row in data]
        self.assertEqual(len(rows), 2)
        self.assertEqual(set(rows[0]), set(BlocklistSerializer.EXPORT_FIELDS))
        data = rows[1]
        expected = [blocklist_item.general_data.title,f"https://www.youtube.com/channel/{blocklist_item.main.id}",
                    str(bl_data.updated_at), user.email, str(bl_data.blocked_count), str(bl_data.unblocked_count)]
        self.assertEqual(data, expected)
