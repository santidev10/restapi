from unittest.mock import patch
import random

import boto3
from django.conf import settings
from elasticsearch.helpers.errors import ScanError
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
from segment.tasks.generate_video_exclusion import failed_callback
from segment.tasks.generate_video_exclusion import LIMIT
from utils.exception import retry
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
        # get_videos_for_channels values lists of lists
        if randomize is True:
            mock_return_values = [
                [[self._video(blocklist=bool(random.randint(0, 1))) for _ in range(random.randint(10, 30))]]
                for _ in range(len(mock_channel_ids))
            ]
        else:
            mock_return_values = [
                [[self._video(blocklist=blocklist) for _ in range(random.randint(10, 30))]]
                for _ in range(len(mock_channel_ids))
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
        mock_return_values = [
            [self._video(blocklist=bool(random.randint(0, 1))) for _ in range(LIMIT + 1)]
        ]
        with patch("segment.tasks.generate_video_exclusion.get_videos_for_channels", return_value=mock_return_values),\
                patch.object(SegmentExporter, "get_extract_export_ids", return_value=[f"yt_channel_{next(int_iterator)}"]):
            video_exclusion_ctl_filename = generate_video_exclusion(self.channel_ctl.id)
        lines = self._get_lines(conn, video_exclusion_ctl_filename)
        self.assertEqual(len(lines), LIMIT)

    @mock_s3
    def test_retry(self):
        """ Test task is retried """
        mock_return_values = [
            [self._video()]
        ]
        with patch.object(SegmentExporter, "get_extract_export_ids", return_value=[f"yt_channel_{next(int_iterator)}"]),\
                patch("segment.tasks.generate_video_exclusion.get_videos_for_channels", return_value=mock_return_values), \
                patch("segment.tasks.generate_video_exclusion._export_results", side_effect=[ConnectionError, None]) as mock_export:
            generate_video_exclusion(self.channel_ctl.id)
        self.assertTrue(mock_export.call_count > 1)

    @mock_s3
    def test_resets_creation_status(self):
        """ Test that ctl video exclusion status resets if task fails """
        @retry(count=0, failed_callback=failed_callback)
        def mock_generate(ctl_id):
            # ctl_id used by failed_callback to reset status
            raise Exception
        try:
            mock_generate(self.channel_ctl.id)
        except Exception:
            pass
        self.channel_ctl.refresh_from_db()
        self.assertFalse(self.channel_ctl.statistics[VideoExclusion.VIDEO_EXCLUSION_FILENAME])
        self.assertFalse(self.channel_ctl.params[VideoExclusion.WITH_VIDEO_EXCLUSION])

    def test_retry_keeps_progress(self):
        """ Test that retrying retrieving videos does not retry the entire task """
        mock_channel_ids, mock_return_values = self._create_mock_args(randomize=True)
        with patch("segment.tasks.generate_video_exclusion._process",
                   side_effect=[ScanError, mock_return_values]) as mock_get, \
                patch.object(SegmentExporter, "get_extract_export_ids", return_value=mock_channel_ids) as mock_get_ids,\
                patch("segment.tasks.generate_video_exclusion._export_results"):
            generate_video_exclusion(self.channel_ctl.id)
            # get_extract_export_ids should only be called once as _process function should be retried
            mock_get_ids.assert_called_once()
