from datetime import date
from unittest.mock import patch

from django.core import mail
from django.db.models.signals import post_save
from django.test import TransactionTestCase

from ads_analyzer.models import OpportunityTargetingReport
from aw_reporting.models import Opportunity
from email_reports.tasks import notify_opportunity_targeting_report_is_ready
from userprofile.models import UserProfile
from utils.utittests.s3_mock import mock_s3
from utils.utittests.str_iterator import str_iterator


class NotifyOpportunityTargetReportTestCase(TransactionTestCase):

    @mock_s3
    def test_notify(self):
        opportunity = Opportunity.objects.create(id=next(str_iterator), name="test Opportunity")
        date_from = date(2019, 1, 2)
        date_to = date(2019, 2, 3)
        recipient = UserProfile.objects.create(email="test@email.com")
        with patch.object(post_save, "send"):
            report = OpportunityTargetingReport.objects.create(
                opportunity=opportunity,
                date_from=date_from,
                date_to=date_to,
                s3_file_key="test_report/key.xlsx"
            )
            report.recipients.add(recipient)

        notify_opportunity_targeting_report_is_ready(report.id)

        self.assertEqual(1, len(mail.outbox))
