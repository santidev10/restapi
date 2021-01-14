from unittest.mock import patch

from .utils import get_params
from .utils import get_test_analyses
from es_components.managers import ChannelManager
from es_components.models import Channel
from performiq.models import IQCampaign
from performiq.analyzers.base_analyzer import ChannelAnalysis
from performiq.analyzers.executor_analyzer import ExecutorAnalyzer
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator


class ExecutorAnalyzerTestCase(ExtendedAPITestCase):
    def _setup(self):
        pass

    def _get_analyses(self, data: list):
        channels = []
        for d in data:
            channel = Channel(f"channel_id_{next(int_iterator)}".zfill(24))
            channel.populate_general_data(iab_categories=d.get("content_categories"), top_lang_code=d.get("lang_code"))
            channel.populate_task_us_data(content_type=d.get("content_type"), content_quality=d.get("content_quality"))
            channels.append(channel)
        analyses = get_test_analyses(channels)
        return analyses

    def test_merge_data_content_categories(self):
        """ Test that content category fields are merged into a single list correctly """
        params = get_params({})
        doc1 = Channel(f"channel_id_{next(int_iterator)}".zfill(24))

        doc2 = Channel(f"channel_id_{next(int_iterator)}".zfill(24))
        doc2.populate_general_data(
            primary_category="Music",
        )

        doc3 = Channel(f"channel_id_{next(int_iterator)}".zfill(24))
        doc3.populate_general_data(
            primary_category="Automobiles",
            iab_categories=["Driving", "Airplanes"]
        )

        doc4 = Channel(f"channel_id_{next(int_iterator)}".zfill(24))
        doc4.populate_general_data(
            iab_categories=["Sports", "Cars"]
        )

        channel_docs = [doc1, doc2, doc3, doc4]
        channel_analyses = [
            ChannelAnalysis(doc.main.id, {}) for doc in channel_docs
        ]
        iq_campaign = IQCampaign.objects.create(params=params)
        with patch.object(ExecutorAnalyzer, "_prepare_data", return_value=[]),\
                patch.object(ChannelManager, "get", return_value=channel_docs):
            executor_analyzer = ExecutorAnalyzer(iq_campaign)
            merged = executor_analyzer._merge_es_data(channel_analyses)
        with self.subTest("Document has no primary category or iab categories"):
            self.assertEqual(set([doc1.general_data.primary_category, *(doc1.general_data.iab_categories or [])]),
                             set(merged[0].get("content_categories")))

        with self.subTest("Document has primary category and has no iab categories"):
            self.assertEqual(set([doc2.general_data.primary_category, *(doc2.general_data.iab_categories or [])]),
                             set(merged[1].get("content_categories")))

        with self.subTest("Document both primary category and iab categories"):
            self.assertEqual(set([doc3.general_data.primary_category, *(doc3.general_data.iab_categories or [])]),
                             set(merged[2].get("content_categories")))

        with self.subTest("Document has no primary category and has iab categories"):
            self.assertEqual(set([doc4.general_data.primary_category, *(doc4.general_data.iab_categories or [])]),
                             set(merged[3].get("content_categories")))
