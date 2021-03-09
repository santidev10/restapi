from unittest.mock import patch
import random

import boto3
from django.conf import settings
from moto import mock_s3

from es_components.models import Video
from es_components.tests.utils import ESTestCase
from segment.models import CustomSegment
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import VideoExclusion
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.tasks.generate_video_exclusion import generate_video_exclusion
from segment.tasks.generate_video_exclusion import LIMIT
from utils.unittests.int_iterator import int_iterator

from utils.unittests.test_case import ExtendedAPITestCase


class GenerateVideoExclusionCTLTestCase(ExtendedAPITestCase, ESTestCase):
    def setUp(self):
        self.channel_ctl = CustomSegment.objects.create(segment_type=SegmentTypeEnum.CHANNEL.value, owner=None)
        CustomSegmentFileUpload.objects.create(segment=self.channel_ctl, query=dict(params={"score_threshold": 4}))

    def _create_bucket(self):
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        return conn

    def _get_lines(self, conn, video_exclusion_ctl):
        export_key = video_exclusion_ctl.export.filename
        lines = conn.Object(settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME, export_key).get()["Body"].read()
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
                [[self._video(blocklist=bool(random.randint(0, 1))) for _ in range(random.randint(10, 50))]]
                for _ in range(len(mock_channel_ids))
            ]
        else:
            mock_return_values = [
                [[self._video(blocklist=blocklist) for _ in range(random.randint(10, 50))]]
                for _ in range(len(mock_channel_ids))
            ]
        return mock_channel_ids, mock_return_values

    @mock_s3
    def test_results(self):
        """ Test that file is exported correctly and results are saved """
        conn = self._create_bucket()
        mock_channel_ids, mock_return_values = self._create_mock_args(randomize=True)
        with patch("segment.tasks.generate_video_exclusion.get_videos_for_channels", side_effect=mock_return_values):
            video_exclusion_ctl = generate_video_exclusion(self.channel_ctl, mock_channel_ids)
        lines = self._get_lines(conn, video_exclusion_ctl)
        self.assertTrue(len(lines) > 1)
        video_exclusion_ctl.refresh_from_db()
        self.channel_ctl.refresh_from_db()
        self.assertEqual(video_exclusion_ctl.statistics[VideoExclusion.CHANNEL_SOURCE_ID], self.channel_ctl.id)

    @mock_s3
    def test_results_blocklist_first(self):
        """ Test that blocklist are sorted first. Blocklist scores are serialized as -1 """
        conn = self._create_bucket()
        mock_channel_ids, mock_return_values = self._create_mock_args(randomize=True)
        with patch("segment.tasks.generate_video_exclusion.get_videos_for_channels", side_effect=mock_return_values):
            video_exclusion_ctl = generate_video_exclusion(self.channel_ctl, mock_channel_ids)
        lines = self._get_lines(conn, video_exclusion_ctl)
        scores = [int(r.split(",")[2]) for r in lines]
        self.assertEqual(scores, list(sorted(scores)))

    @mock_s3
    def test_results_sorted(self):
        """ Test that results are sorted ascending brand safety score """
        conn = self._create_bucket()
        mock_channel_ids, mock_return_values = self._create_mock_args()
        with patch("segment.tasks.generate_video_exclusion.get_videos_for_channels", side_effect=mock_return_values):
            video_exclusion_ctl = generate_video_exclusion(self.channel_ctl, mock_channel_ids)
        lines = self._get_lines(conn, video_exclusion_ctl)
        scores = [int(r.split(",")[2]) for r in lines]
        self.assertEqual(scores, list(sorted(scores)))

    @mock_s3
    def test_results_truncated(self):
        """ Test that results are truncated at limit """
        conn = self._create_bucket()
        mock_return_values = [
            [[self._video(blocklist=bool(random.randint(0, 1))) for _ in range(LIMIT + 1)]]
        ]
        with patch("segment.tasks.generate_video_exclusion.get_videos_for_channels", side_effect=mock_return_values):
            video_exclusion_ctl = generate_video_exclusion(self.channel_ctl, [f"yt_channel_{next(int_iterator)}"])
        lines = self._get_lines(conn, video_exclusion_ctl)
        self.assertEqual(len(lines), LIMIT)

    def _video(self, blocklist=False):
        video = Video(f"video_{next(int_iterator)}")
        video.populate_brand_safety(overall_score=random.randint(0, 100))
        video.populate_custom_properties(blocklist=blocklist)
        return video
