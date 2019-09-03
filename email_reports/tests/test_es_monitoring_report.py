from unittest.mock import patch

from django.test import TestCase
from django.core import mail

from email_reports.tasks import send_daily_email_reports
from es_components.tests.utils import ESTestCase
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import KeywordManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Keyword
from es_components.models import Video


class ESMonitoringTestCase(TestCase, ESTestCase):

    @patch("email_reports.reports.es_monitoring_report.settings.ES_MONITORING_EMAIL_ADDRESSES", ["test@test.test"])
    def test_send_email(self):

        channels = [Channel("monitoring" + str(i)) for i in range(10)]
        ChannelManager((Sections.GENERAL_DATA, Sections.STATS)).upsert(channels)

        videos = [Video("monitoring" + str(i)) for i in range(10)]
        VideoManager((Sections.GENERAL_DATA, Sections.STATS)).upsert(videos)

        keywords = [Keyword("monitoring" + str(i)) for i in range(10)]
        KeywordManager(Sections.STATS).upsert(keywords)

        send_daily_email_reports(reports=["ESMonitoringEmailReport"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        text_body = email.body

        self.assertIn("ElasticSearch Monitoring Report", text_body)
        self.assertIn("channels", text_body)
        self.assertIn("videos", text_body)
        self.assertIn("keywords", text_body)