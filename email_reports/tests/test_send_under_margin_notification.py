from datetime import timedelta

from django.conf import settings
from django.core import mail

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import User
from email_reports.tasks import send_daily_email_reports
from utils.datetime import now_in_default_tz
from utils.unittests.patch_now import patch_now
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase


class SendDailyEmailsTestCase(APITestCase):

    def test_send_minimum_email(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        now = now_in_default_tz()
        today = now.date()
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

        with patch_now(now):
            send_daily_email_reports(reports=["CampaignUnderMargin"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        expected_subject = f"{ad_ops.name} Opportunities Under Margin Report"
        self.assertEqual(mail.outbox[0].subject, expected_subject)
        self.assertEqual(len(mail.outbox[0].to), 1)
        self.assertEqual(mail.outbox[0].to[0], ad_ops.email)
        self.assertEqual(mail.outbox[0].from_email, settings.EXPORTS_EMAIL_ADDRESS)

    def test_send_to_account_by_timezone(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        now = now_in_default_tz()
        today = now.date()

        account = Account.objects.create(id=1, timezone=settings.DEFAULT_TIMEZONE)
        opportunity = Opportunity.objects.create(
            id=1, name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
            budget=100,
            aw_cid=account.id
        )
        placement = OpPlacement.objects.create(
            id=1,
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
        stats = dict(cost=.5, video_views=10)
        campaign = Campaign.objects.create(pk="1", name="", account=account,
                                           salesforce_placement=placement,
                                           **stats)
        CampaignStatistic.objects.create(campaign=campaign, date=flight.start,
                                         **stats)

        now_manila_timezone = now_in_default_tz(tz_str="Asia/Manila")
        today_manila_timezone = now_manila_timezone.date()

        account_2 = Account.objects.create(id=2, timezone="Asia/Manila")
        opportunity_2 = Opportunity.objects.create(
            id=2, name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today_manila_timezone - timedelta(days=2),
            end=today_manila_timezone + timedelta(days=2),
            probability=100,
            budget=100,
            aw_cid=account_2.id
        )
        placement_2 = OpPlacement.objects.create(
            id=2,
            name="Placement",
            start=today_manila_timezone - timedelta(days=2),
            end=today_manila_timezone + timedelta(days=2),
            opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=.05,
        )
        flight_2 = Flight.objects.create(id=2, name="",
                                         placement=placement_2,
                                         start=today_manila_timezone.replace(day=1),
                                         end=today_manila_timezone.replace(day=28),
                                         ordered_units=1000,
                                         total_cost=100)
        stats = dict(cost=.5, video_views=10)
        campaign_2 = Campaign.objects.create(pk=2, name="", account=account_2,
                                             salesforce_placement=placement_2,
                                             **stats)
        CampaignStatistic.objects.create(campaign=campaign_2, date=flight_2.start,
                                         **stats)

        with patch_now(now_in_default_tz(tz_str="America/Vancouver")):
            # check if no mails will be sent for non existence timezone
            send_daily_email_reports(reports=["CampaignUnderMargin"], debug=False, timezone_name="America/Vancouver")
        self.assertEqual(len(mail.outbox), 0)

        with patch_now(now):
            send_daily_email_reports(reports=["CampaignUnderMargin"], debug=False,
                                     timezone_name=settings.DEFAULT_TIMEZONE)
        self.assertEqual(len(mail.outbox), 1)

    def test_receivers_no_sales(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        sm = User.objects.create(id="2", name="David", email="2@mail.cz")
        now = now_in_default_tz()
        today = now.date()

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

        with patch_now(now):
            send_daily_email_reports(reports=["CampaignUnderMargin"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        self.assertEqual(email.from_email, settings.EXPORTS_EMAIL_ADDRESS)
        receivers = email.to + email.cc + email.bcc
        receivers_mails = (r[1] if isinstance(r, tuple) else r
                           for r in receivers)
        self.assertNotIn(sm.email, receivers_mails)
