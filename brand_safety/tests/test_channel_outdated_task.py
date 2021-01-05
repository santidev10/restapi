from datetime import timedelta
from mock import patch
from mock import MagicMock

from django.utils import timezone
from elasticsearch_dsl import Q

from audit_tool.models import AuditLanguage
from brand_safety.models import BadWordCategory
from brand_safety.models import BadWord
from brand_safety.tasks.channel_outdated import channel_outdated_scheduler
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.tests.utils import ESTestCase
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class ChannelOutdatedTestCase(ExtendedAPITestCase, ESTestCase):
    channel_manager = ChannelManager(sections=[Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.TASK_US_DATA])
    video_manager = VideoManager(sections=[Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.TASK_US_DATA])

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        lang = AuditLanguage.objects.get_or_create(language="test")[0]
        category = BadWordCategory.objects.get_or_create(name="test")[0]
        BadWord.objects.get_or_create(language=lang, category=category)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_channel_outdated_scheduler(self):
        """ Test scheduler should retrieve channels outdated brand safety data """
        outdated = timezone.now() - timedelta(days=30)
        doc_has_task_us_data = Channel(
            id=f"test_channel_{next(int_iterator)}",
            task_us_data={"last_vetted": timezone.now()}
        )
        doc_outdated_bs_has_task_us_data = Channel(
            id=f"test_channel_{next(int_iterator)}",
            task_us_data={"last_vetted": timezone.now()},
            brand_safety={"overall_score": 100}
        )
        doc_outdated_bs_no_task_us_data = Channel(
            id=f"test_channel_{next(int_iterator)}",
            brand_safety={"overall_score": 100},
        )
        with patch.object(ChannelManager, "forced_filters", return_value=Q({"bool": {}})),\
                patch("es_components.managers.base.datetime_service.now", return_value=outdated),\
                patch("brand_safety.tasks.channel_outdated.channel_update_helper") as helper_mock, \
                patch("brand_safety.tasks.channel_outdated.get_queue_size", return_value=0), \
                patch("utils.celery.tasks.REDIS_CLIENT") as mock_redis:
            mock_lock = MagicMock()
            mock_lock.acquire.return_value = True
            mock_redis.lock.return_value = mock_lock
            self.channel_manager.upsert([doc_has_task_us_data, doc_outdated_bs_has_task_us_data])
            self.channel_manager.upsert_sections = [Sections.MAIN, Sections.BRAND_SAFETY]
            self.channel_manager.upsert([doc_outdated_bs_no_task_us_data])
            channel_outdated_scheduler.run()
            query = helper_mock.call_args.args[1]
            channels = self.channel_manager.search(query).execute()
        response_ids = [c.main.id for c in channels]
        self.assertTrue(doc_has_task_us_data.main.id in response_ids)
        self.assertTrue(doc_outdated_bs_has_task_us_data.main.id in response_ids)
        self.assertTrue(doc_outdated_bs_no_task_us_data.main.id in response_ids)

    def test_outdated_task_success(self):
        """ Test task runs to completion successfully """
        outdated = timezone.now() - timedelta(days=30)
        doc_outdated = Channel(
            id=f"test_channel_{next(int_iterator)}",
            brand_safety={"overall_score": 100},
        )
        channel_manager = ChannelManager(upsert_sections=[Sections.BRAND_SAFETY])
        with patch("es_components.managers.base.datetime_service.now", return_value=outdated):
            channel_manager.upsert([doc_outdated])
        with patch.object(ChannelManager, "forced_filters", return_value=Q({"bool": {}})), \
            patch("brand_safety.tasks.channel_update_helper.get_queue_size", return_value=0), \
            patch("brand_safety.tasks.channel_outdated.get_queue_size", return_value=0), \
            patch("utils.celery.tasks.REDIS_CLIENT") as mock_redis:
                mock_lock = MagicMock()
                mock_lock.acquire.return_value = True
                mock_redis.lock.return_value = mock_lock

                channel_outdated_scheduler.run()
        updated = self.channel_manager.get([doc_outdated.main.id])[0]
        self.assertTrue(updated.brand_safety.updated_at > doc_outdated.brand_safety.updated_at)
