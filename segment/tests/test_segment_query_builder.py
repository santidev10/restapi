from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.tests.utils import ESTestCase
from utils.unittests.int_iterator import int_iterator
from segment.utils.query_builder import SegmentQueryBuilder


class SegmentQueryBuilderTestCase(TestCase, ESTestCase):
    def setUp(self):
        sections = [Sections.TASK_US_DATA, Sections.STATS, Sections.ADS_STATS, Sections.GENERAL_DATA]
        self.channel_manager = ChannelManager(sections=sections, upsert_sections=sections)
        self.video_manager = VideoManager(sections=sections, upsert_sections=sections)

    def test_video_vetted_after(self):
        """ Should return documents vetted after a provided date """
        today = timezone.now().date()
        before = today - timedelta(days=1)
        after = today + timedelta(days=1)
        vetted_before = []
        vetted_after = []
        for i in range(10):
            doc = self.video_manager.model(f"video_{next(int_iterator)}")
            doc.populate_general_data(lang_code="en")
            if i % 2 == 0:
                doc.populate_task_us_data(lang_code="en", last_vetted_at=before)
                vetted_before.append(doc)
            else:
                doc.populate_task_us_data(lang_code="en", last_vetted_at=after)
                doc.populate_task_us_data(lang_code="en")
                vetted_after.append(doc)
        self.video_manager.upsert(vetted_before)
        self.video_manager.upsert(vetted_after)

        data = dict(vetted_after=today.strftime("%Y-%m-%d"), segment_type=0)
        query_builder = SegmentQueryBuilder(data, with_forced_filters=False)
        response = query_builder.execute()
        expected = {doc.main.id for doc in vetted_after}
        not_expected = {doc.main.id for doc in vetted_before}
        received = {doc.main.id for doc in response}
        self.assertEqual(expected, received)
        self.assertNotEqual(not_expected, received)

    def test_channel_vetted_after(self):
        """ Should return documents vetted after a provided date """
        today = timezone.now().date()
        before = today - timedelta(days=1)
        after = today + timedelta(days=1)
        vetted_before = []
        vetted_after = []
        for i in range(10):
            doc = self.channel_manager.model(f"channel_{next(int_iterator)}")
            doc.populate_general_data(top_lang_code="en")
            if i % 2 == 0:
                doc.populate_task_us_data(lang_code="en", last_vetted_at=before)
                vetted_before.append(doc)
            else:
                doc.populate_task_us_data(lang_code="en", last_vetted_at=after)
                vetted_after.append(doc)

        self.channel_manager.upsert(vetted_before)
        self.channel_manager.upsert(vetted_after)

        data = dict(vetted_after=today.strftime("%Y-%m-%d"), segment_type=1)
        query_builder = SegmentQueryBuilder(data, with_forced_filters=False)
        response = query_builder.execute()
        expected = {doc.main.id for doc in vetted_after}
        not_expected = {doc.main.id for doc in vetted_before}
        received = {doc.main.id for doc in response}
        self.assertEqual(expected, received)
        self.assertNotEqual(not_expected, received)

    def test_ads_stats(self):
        doc1 = self.channel_manager.model(f"channel_{next(int_iterator)}")
        doc2 = self.channel_manager.model(f"channel_{next(int_iterator)}")
        doc3 = self.channel_manager.model(f"channel_{next(int_iterator)}")
        doc1.populate_ads_stats(
            video_view_rate=1,
            average_cpv=1,
            average_cpm=1,
            ctr=1,
            ctr_v=1,
            video_quartile_100_rate=1,
        )
        doc1.populate_stats(last_30day_views=1)
        doc2.populate_ads_stats(
            video_view_rate=1,
            average_cpv=1,
            average_cpm=1,
            ctr=2,
            ctr_v=2,
            video_quartile_100_rate=2,
        )
        doc2.populate_stats(last_30day_views=2)
        doc3.populate_ads_stats(
            video_view_rate=2,
            average_cpv=2,
            average_cpm=2,
            ctr=3,
            ctr_v=3,
            video_quartile_100_rate=3,
        )
        doc3.populate_stats(last_30day_views=3)
        self.channel_manager.upsert([doc1, doc2, doc3])
        params = dict(
            video_view_rate="1,2",
            average_cpv="1,2",
            average_cpm="1,2",
            ctr="1,2",
            ctr_v="1,2",
            video_quartile_100_rate="1,2",
            last_30day_views="1,2",
        )
        query_builder = SegmentQueryBuilder(params, with_forced_filters=False)
        response = query_builder.execute()
        self.assertEqual(len(response), 2)
        self.assertEqual({doc.main.id for doc in response}, {doc1.main.id, doc2.main.id})

    def test_content_exclusions(self):
        doc1 = self.channel_manager.model(f"channel_{next(int_iterator)}")
        doc2 = self.channel_manager.model(f"channel_{next(int_iterator)}")
        doc3 = self.channel_manager.model(f"channel_{next(int_iterator)}")
        doc4 = self.channel_manager.model(f"channel_{next(int_iterator)}")
        doc5 = self.channel_manager.model(f"channel_{next(int_iterator)}")
        doc1.populate_general_data(
            iab_categories=["Car Culture", "Motorcycles"]
        )
        doc2.populate_general_data(
            iab_categories=["Motorcycles"]
        )
        doc3.populate_general_data(
            iab_categories=["Car Culture", "Auto Technology"]
        )
        doc4.populate_general_data(
            iab_categories=["Motorcycles", "Auto Safety"]
        )
        doc5.populate_general_data(
            primary_category="Motorcycles"
        )
        self.channel_manager.upsert([doc1, doc2, doc3])
        params = dict(
            content_categories=["Car Culture", "Auto Technology"],
            exclude_content_categories=["Motorcycles"],
        )
        query_builder = SegmentQueryBuilder(params, with_forced_filters=False)
        response = query_builder.execute()
        self.assertEqual(len(response), 1)
        self.assertEqual(response[0].main.id, doc3.main.id)
