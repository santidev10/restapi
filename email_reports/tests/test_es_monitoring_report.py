from unittest.mock import patch

from django.core import mail
from django.test import TestCase

from email_reports.tasks import send_daily_email_reports
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import KeywordManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Keyword
from es_components.models import Video
from es_components.tests.utils import ESTestCase


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
        self.assertIsNotNone(email.alternatives[0][0])

    @patch("email_reports.reports.es_monitoring_report.settings.ES_MONITORING_EMAIL_ADDRESSES", ["test@test.test"])
    def test_send_email_no_docs_found(self):
        send_daily_email_reports(reports=["ESMonitoringEmailReport"], debug=False)

        self.assertEqual(len(mail.outbox), 3)
        email_multi_alternatives = mail.outbox[-1]
        self.assertIn(
            "Less than 1% of general_data, stats data has been updated during the last day", mail.outbox[0].body
        )

        self.assertIn(
            "Less than 1% of general_data, stats data has been updated during the last day", mail.outbox[1].body
        )

        self.assertIsNotNone(email_multi_alternatives.alternatives[0][0])
