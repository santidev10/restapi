from unittest.mock import patch

from elasticsearch.helpers.errors import BulkIndexError

from es_components.models import Video
from es_components.tests.utils import ESTestCase
from es_components.managers import VideoManager
from utils.exception import upsert_retry
from utils.unittests.int_iterator import int_iterator


class UpsertRetryTestCase(ESTestCase):
    def test_retry(self):
        """ Test that function retries with exception docs """
        video_manager = VideoManager()
        videos = [
            Video(f"video_{next(int_iterator)}") for _ in range(50)
        ]
        failed_docs = [v for i, v in enumerate(videos) if i % 2 == 0]
        exc_errors = [
            {
                "update": {
                    "_id": doc.main.id
                }
            }
            for doc in failed_docs
        ]
        mock_exc = BulkIndexError(
            "%i document(s) failed to index." % len(exc_errors), exc_errors
        )
        with patch.object(VideoManager, "upsert", side_effect=[mock_exc, None]) as mock_upsert:
            upsert_retry(video_manager, videos)
            self.assertEqual(mock_upsert.call_count, 2)
