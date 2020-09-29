from mock import patch

from elasticsearch_dsl import Q

from brand_safety.auditors.video_auditor import VideoAuditor
from brand_safety.tasks.constants import Schedulers
from brand_safety.tasks.video_discovery import video_discovery_scheduler
from brand_safety.tasks.video_discovery import video_update
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
                patch("brand_safety.tasks.video_discovery.VideoAuditor") as mock_audit:
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

    def test_scheduler_runs(self):
        """ Test Video discovery scheduler task runs when queue is below threshold """
        threshold = Schedulers.VideoDiscovery.get_minimum_threshold() - 1
        with patch("brand_safety.tasks.video_discovery.get_queue_size", return_value=threshold),\
            patch("brand_safety.tasks.video_discovery.group") as group_mock:
            video_discovery_scheduler()
            group_mock.assert_called_once()

    def test_scheduler_not_runs(self):
        """ Test Video discovery scheduler task does not run when queue is above threshold """
        threshold = Schedulers.VideoDiscovery.get_minimum_threshold() + 1
        with patch("brand_safety.tasks.video_discovery.get_queue_size", return_value=threshold),\
            patch("brand_safety.tasks.video_discovery.group") as group_mock:
            video_discovery_scheduler()
            group_mock.assert_not_called()

    def test_discovery_rescore_scheduler(self):
        """ Test discovery scheduler schedules rescore videos """
        video = Video(f"v_{next(int_iterator)}")
        video.populate_general_data(
            title="test title",
            description="test description",
        )
        video.populate_brand_safety(overall_score=0, rescore=True)
        self.video_manager.upsert([video])
        with patch("brand_safety.tasks.video_discovery.video_update.apply") as mock_update, \
                patch("brand_safety.tasks.video_discovery.get_queue_size", return_value=0):
            video_discovery_scheduler()
        ids_arg, rescore_kwarg = mock_update.call_args.args
        self.assertEqual({video.main.id}, set(*ids_arg))
        self.assertEqual(rescore_kwarg["rescore"], True)

    def test_update_resets_rescore(self):
        """ Test discovery scheduler updates rescore value after scoring """
        video = Video(f"v_{next(int_iterator)}")
        video.populate_general_data(
            title="test title",
            description="test description",
        )
        video.populate_brand_safety(overall_score=0, rescore=True)
        self.video_manager.upsert([video])
        with patch.object(VideoAuditor, "process"):
            video_update([video.main.id], rescore=True)
            updated = self.video_manager.get([video.main.id])[0]
            self.assertEqual(updated.brand_safety.rescore, False)