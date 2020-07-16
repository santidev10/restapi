from mock import patch
from elasticsearch_dsl import Q

from brand_safety.tasks.channel_discovery import channel_discovery_scheduler
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.tests.utils import ESTestCase
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class ChannelDiscoveryTestCase(ExtendedAPITestCase, ESTestCase):
    channel_manager = ChannelManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY))
    video_manager = VideoManager(sections=(Sections.GENERAL_DATA, Sections.BRAND_SAFETY))

    def test_channel_discovery_scheduler(self):
        """ Test scheduler should retrieve channels with no brand safety or rescore = True"""
        doc_excluded_has_bs = Channel(
            id=f"test_channel_{next(int_iterator)}",
            brand_safety={"overall_score": 100},
        )
        doc_excluded_rescore_false = Channel(
            id=f"test_channel_{next(int_iterator)}",
            brand_safety={"overall_score": 100}
        )
        doc_included_rescore_true = Channel(
            id=f"test_channel_{next(int_iterator)}",
            brand_safety={"rescore": True}
        )
        doc_included_has_no_bs = Channel(
            id=f"test_channel_{next(int_iterator)}",
        )
        self.channel_manager.upsert([doc_excluded_has_bs, doc_excluded_rescore_false,
                                     doc_included_rescore_true, doc_included_has_no_bs])
        with patch.object(ChannelManager, "forced_filters", return_value=Q({"bool": {}})),\
                patch("brand_safety.tasks.channel_discovery.channel_update_helper") as helper_mock:
            channel_discovery_scheduler()
            query = helper_mock.call_args.args[1]
            channels = self.channel_manager.search(query).execute()
        response_ids = [c.main.id for c in channels]
        self.assertTrue(doc_excluded_has_bs.main.id not in response_ids)
        self.assertTrue(doc_excluded_rescore_false.main.id not in response_ids)
        self.assertTrue(doc_included_rescore_true.main.id in response_ids)
        self.assertTrue(doc_included_has_no_bs.main.id in response_ids)
