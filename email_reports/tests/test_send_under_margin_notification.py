from datetime import timedelta

from django.conf import settings
from django.core import mail
from django.utils import timezone

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import User
from email_reports.tasks import send_daily_email_reports
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase


class SendDailyEmailsTestCase(APITestCase):

    def test_send_minimum_email(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
            budget=100,
        )
        placement = OpPlacement.objects.create(
            id="1",
            name="Placement",
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=.05,
        )
        flight = Flight.objects.create(id="1", name="",
                                       placement=placement,
                                       start=today.replace(day=1),
                                       end=today.replace(day=28),
                                       ordered_units=1000,
                                       total_cost=100)
        account = Account.objects.create(pk="1", name="")
        stats = dict(cost=.5, video_views=10)
        campaign = Campaign.objects.create(pk="1", name="", account=account,
                                           salesforce_placement=placement,
                                           **stats)
        CampaignStatistic.objects.create(campaign=campaign, date=flight.start,
                                         **stats)

        send_daily_email_reports(reports=["CampaignUnderMargin"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        expected_subject = "URGENT CAMPAIGN UNDER MARGIN: {}".format(
            opportunity.name)
        self.assertEqual(mail.outbox[0].subject, expected_subject)
        self.assertEqual(len(mail.outbox[0].to), 1)
        self.assertEqual(mail.outbox[0].to[0], ad_ops.email)
        self.assertEqual(mail.outbox[0].from_email, settings.EXPORTS_EMAIL_ADDRESS)

    def test_receivers_no_sales(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        sm = User.objects.create(id="2", name="David", email="2@mail.cz")

        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            sales_manager=sm,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
            budget=100,
        )
        placement = OpPlacement.objects.create(
            id="1",
            name="Placement",
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=.05,
        )
        flight = Flight.objects.create(id="1", name="",
                                       placement=placement,
                                       start=today.replace(day=1),
                                       end=today.replace(day=28),
                                       ordered_units=1000,
                                       total_cost=100)
        account = Account.objects.create(pk="1", name="")
        stats = dict(cost=.5, video_views=10)
        campaign = Campaign.objects.create(pk="1", name="", account=account,
                                           salesforce_placement=placement,
                                           **stats)
        CampaignStatistic.objects.create(campaign=campaign, date=flight.start,
                                         **stats)

        send_daily_email_reports(reports=["CampaignUnderMargin"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        self.assertEqual(email.from_email, settings.EXPORTS_EMAIL_ADDRESS)
        receivers = email.to + email.cc + email.bcc
        receivers_mails = (r[1] if isinstance(r, tuple) else r
                           for r in receivers)
        self.assertNotIn(sm.email, receivers_mails)
