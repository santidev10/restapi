from uuid import uuid4
from collections import defaultdict

from django.test import TransactionTestCase

from segment.models import CustomSegment
from segment.models.utils.generate_segment_utils import GenerateSegmentUtils


class GenerateSegmentUtilsTestCase(TransactionTestCase):
    normalized_likes_key = "likes"
    normalized_dislikes_key = "dislikes"
    channel_likes_key = "observed_videos_likes"
    channel_dislikes_key = "observed_videos_dislikes"

    def _create_segment(self):
        return CustomSegment.objects.create(title=uuid4(), segment_type=0, list_type=0, uuid=uuid4())

    def test_that_channel_aggregations_are_normalized_correctly(self):
        """
        channel aggregations stats keys should be normalized. "likes" and "dislikes" should be present
        :return:
        """
        aggregations = defaultdict(int)
        likes_count = 117
        dislikes_count = 343
        aggregations[self.channel_likes_key] = likes_count
        aggregations[self.channel_dislikes_key] = dislikes_count

        segment = self._create_segment()
        generate_utils = GenerateSegmentUtils(segment)
        generate_utils.finalize_aggregations(aggregations, 1)
        aggregations_keys = aggregations.keys()
        self.assertIn(self.normalized_likes_key, aggregations_keys)
        self.assertIn(self.normalized_dislikes_key, aggregations_keys)
        self.assertNotIn(self.channel_likes_key, aggregations_keys)
        self.assertNotIn(self.channel_dislikes_key, aggregations_keys)
        self.assertEqual(likes_count, aggregations.get(self.normalized_likes_key, None))
        self.assertEqual(dislikes_count, aggregations.get(self.normalized_dislikes_key, None))

    def test_that_video_aggregations_unaffected_by_channel_key_normalization(self):
        """
        video aggregations stats keys should be unaffected by channel key normalization
        :return:
        """
        aggregations = defaultdict(int)
        likes_count = 117
        dislikes_count = 343
        aggregations[self.normalized_likes_key] = likes_count
        aggregations[self.normalized_dislikes_key] = dislikes_count

        segment = self._create_segment()
        generate_utils = GenerateSegmentUtils(segment)
        generate_utils.finalize_aggregations(aggregations, 1)
        self.assertEqual(likes_count, aggregations.get(self.normalized_likes_key, None))
        self.assertEqual(dislikes_count, aggregations.get(self.normalized_dislikes_key, None))
