from unittest.mock import patch
import random

import boto3
from django.conf import settings
from elasticsearch.exceptions import ConnectionError
from moto import mock_s3

from es_components.models import Video
from es_components.tests.utils import ESTestCase
from segment.models import CustomSegment
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import VideoExclusion
from segment.models.utils.segment_exporter import SegmentExporter
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.tasks.generate_video_exclusion import generate_video_exclusion
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class GenerateVideoExclusionCTLTestCase(ExtendedAPITestCase, ESTestCase):
    def setUp(self):
        self.channel_ctl = CustomSegment.objects.create(segment_type=SegmentTypeEnum.CHANNEL.value, params={
            VideoExclusion.VIDEO_EXCLUSION_SCORE_THRESHOLD: 2,
            VideoExclusion.WITH_VIDEO_EXCLUSION: True,
        })
        CustomSegmentFileUpload.objects.create(segment=self.channel_ctl, query=dict(params={"score_threshold": 4}))

    def _video(self, blocklist=False):
        video = Video(f"video_{next(int_iterator)}")
        video.populate_brand_safety(overall_score=random.randint(0, 100))
        video.populate_custom_properties(blocklist=blocklist)
        return video

    def _create_bucket(self):
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        return conn

    def _get_lines(self, conn, video_exclusion_filename):
        lines = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, video_exclusion_filename).get()["Body"].read()
        # Skip header
        lines = lines.decode('utf-8').split()[1:]
        return lines

    def _create_mock_args(self, blocklist=False, randomize=False):
        """
        :param blocklist:
        :param randomize: bool -> Create mock_return_values with random blocklist=True values
        :return:
        """
        mock_channel_ids = [
            f"yt_channel_{next(int_iterator)}" for _ in range(random.randint(5, 10))
        ]
        if randomize is True:
            mock_return_values = [
                [self._video(blocklist=bool(random.randint(0, 1))) for _ in range(random.randint(10, 30))]
            ]
        else:
            mock_return_values = [
                [self._video(blocklist=blocklist) for _ in range(random.randint(10, 30))]
            ]
        return mock_channel_ids, mock_return_values

    @mock_s3
    def test_results(self):
        """ Test that file is exported correctly and results are saved """
        conn = self._create_bucket()
        mock_channel_ids, mock_return_values = self._create_mock_args(randomize=True)
        with patch("segment.tasks.generate_video_exclusion.get_videos_for_channels", side_effect=mock_return_values),\
                patch.object(SegmentExporter, "get_extract_export_ids", return_value=mock_channel_ids):
            video_exclusion_filename = generate_video_exclusion(self.channel_ctl.id)
        self.channel_ctl.refresh_from_db()
        lines = self._get_lines(conn, video_exclusion_filename)
        self.assertTrue(len(lines) > 1)
        self.assertTrue(self.channel_ctl.statistics[VideoExclusion.VIDEO_EXCLUSION_FILENAME])

    @mock_s3
    def test_results_truncated(self):
        """ Test that results are truncated at limit """
        conn = self._create_bucket()
        TEST_LIMIT = 1000
        mock_return_values = [
            self._video(blocklist=True) for _ in range(TEST_LIMIT + 1)
        ]
        with patch("segment.tasks.generate_video_exclusion.get_videos_for_channels", return_value=mock_return_values),\
                patch.object(SegmentExporter, "get_extract_export_ids", return_value=[f"yt_channel_{next(int_iterator)}"]),\
                patch("segment.tasks.generate_video_exclusion.LIMIT", TEST_LIMIT):
            video_exclusion_ctl_filename = generate_video_exclusion(self.channel_ctl.id)
        lines = self._get_lines(conn, video_exclusion_ctl_filename)
        self.assertEqual(len(lines), TEST_LIMIT)

    @mock_s3
    def test_retry(self):
        """ Test task is retried """
        mock_return_values = [
            self._video()
        ]
        with patch.object(SegmentExporter, "get_extract_export_ids", return_value=[f"yt_channel_{next(int_iterator)}"]),\
                patch("segment.tasks.generate_video_exclusion.get_videos_for_channels", return_value=mock_return_values), \
                patch("segment.tasks.generate_video_exclusion._export_results", side_effect=[ConnectionError, None]) as mock_export:
            generate_video_exclusion(self.channel_ctl.id)
        self.assertTrue(mock_export.call_count > 1)

    @mock_s3
    def test_retry_keeps_progress(self):
        """ Test that retrying retrieving videos does not retry the entire task """
        mock_channel_ids, mock_return_values = self._create_mock_args(randomize=True)
        with patch("segment.tasks.generate_video_exclusion.search_after",
                   side_effect=[ConnectionError, mock_return_values]) as mock_search_after, \
                patch.object(SegmentExporter, "get_extract_export_ids", return_value=mock_channel_ids) as mock_get_ids,\
                patch.object(CustomSegment, "save"),\
                patch("segment.tasks.generate_video_exclusion._export_results"):
            generate_video_exclusion(self.channel_ctl.id)
            # get_extract_export_ids should only be called once as _process function should be retried
            mock_get_ids.assert_called_once()
