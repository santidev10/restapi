from mock import patch
from datetime import timedelta

from django.test.testcases import TestCase
from django.utils import timezone

from brand_safety.utils import BrandSafetyQueryBuilder
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.tests.utils import ESTestCase
from utils.unittests.int_iterator import int_iterator


class BrandSafetyQueryBuilderTestCase(TestCase, ESTestCase):
    def setUp(self):
        sections = [Sections.TASK_US_DATA]
        self.channel_manager = ChannelManager(sections=sections)
        self.video_manager = VideoManager(sections=sections)

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
            doc.populate_task_us_data(lang_code="en")
            if i % 2 == 0:
                vetted_before.append(doc)
            else:
                vetted_after.append(doc)
        with patch("es_components.managers.base.datetime_service") as mock_date:
            mock_date.now.return_value = before
            self.video_manager.upsert(vetted_before)
        with patch("es_components.managers.base.datetime_service") as mock_date:
            mock_date.now.return_value = after
            self.video_manager.upsert(vetted_after)

        data = dict(vetted_after=today.strftime("%Y-%m-%d"), segment_type=0)
        query_builder = BrandSafetyQueryBuilder(data, with_forced_filters=False)
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
            doc.populate_task_us_data(lang_code="en")
            if i % 2 == 0:
                vetted_before.append(doc)
            else:
                vetted_after.append(doc)
        with patch("es_components.managers.base.datetime_service") as mock_date:
            mock_date.now.return_value = before
            self.channel_manager.upsert(vetted_before)
        with patch("es_components.managers.base.datetime_service") as mock_date:
            mock_date.now.return_value = after
            self.channel_manager.upsert(vetted_after)

        data = dict(vetted_after=today.strftime("%Y-%m-%d"), segment_type=1)
        query_builder = BrandSafetyQueryBuilder(data, with_forced_filters=False)
        response = query_builder.execute()
        expected = {doc.main.id for doc in vetted_after}
        not_expected = {doc.main.id for doc in vetted_before}
        received = {doc.main.id for doc in response}
        self.assertEqual(expected, received)
        self.assertNotEqual(not_expected, received)
