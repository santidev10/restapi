from mock import patch

from elasticsearch_dsl import Q

from brand_safety.tasks.video_discovery import video_discovery_scheduler
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class VideoBrandSafetyTestCase(ExtendedAPITestCase, ESTestCase):
    channel_manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY))
    video_manager = VideoManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY))

    def test_video_discovery_channel_rescore(self):
        """
        Test video discovery and channel rescore flow
        Channels to rescore by video discovery should have rescore flag set to True
        """
        rescore_channels = [
            Channel(f"test_channel_{next(int_iterator)}"),
            Channel(f"test_channel_{next(int_iterator)}")
        ]
        rescore_channel_ids = [c.main.id for c in rescore_channels]
        doc_excluded_has_bs = Video(
            id=f"test_video_{next(int_iterator)}",
            brand_safety={"overall_score": 100},
        )
        doc_excluded_rescore_false = Video(
            id=f"test_video_{next(int_iterator)}",
            brand_safety={"overall_score": 100}
        )
        doc_included_rescore_true = Video(
            id=f"test_video_{next(int_iterator)}",
            brand_safety={"rescore": True}
        )
        doc_included_has_no_bs = Video(
            id=f"test_video_{next(int_iterator)}",
        )
        self.video_manager.upsert([doc_excluded_has_bs, doc_excluded_rescore_false,
                                   doc_included_rescore_true, doc_included_has_no_bs])
        with patch.object(VideoManager, "forced_filters", return_value=Q({"bool": {}})),\
                patch.object(VideoManager, "search") as mock_search,\
                patch("brand_safety.tasks.video_discovery.get_queue_size", return_value=0),\
                patch("brand_safety.tasks.video_discovery.BrandSafetyAudit") as mock_audit:
            instance = mock_audit.return_value
            instance.channels_to_rescore = rescore_channel_ids
            video_discovery_scheduler()
            query = mock_search.call_args.args[0]
            # Use protected method instead of search method to bypass patch.Object statement
            search = self.video_manager._search()
            search.query = query
            videos = search.execute()
            response_ids = [v.main.id for v in videos]
            channels = self.channel_manager.get(rescore_channel_ids, skip_none=True)
        self.assertTrue(doc_excluded_has_bs.main.id not in response_ids)
        self.assertTrue(doc_excluded_rescore_false.main.id not in response_ids)
        self.assertTrue(doc_included_rescore_true.main.id in response_ids)
        self.assertTrue(doc_included_has_no_bs.main.id in response_ids)

        rescore_flags = [c.brand_safety.rescore for c in channels]
        self.assertEqual([True for _ in channels], rescore_flags)

