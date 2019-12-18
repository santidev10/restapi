from datetime import timedelta

from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
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
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase


class SendDailyEmailsTestCase(APITestCase):

    def test_send_minimum_email(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        sales = User.objects.create(id="2", name="Dave", email="2@mail.cz")
        acc_mng = User.objects.create(id="3", name="John", email="3@mail.cz")
        today = now_in_default_tz().date()
        start, end = today - timedelta(days=2), today + timedelta(days=3)

        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            account_manager=acc_mng,
            sales_manager=sales,
            start=start,
            end=end,
            probability=100,
            budget=100,
        )
        placement = OpPlacement.objects.create(
            id="1",
            name="Placement",
            start=start,
            end=end,
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=.05,
        )

        flight_data = dict(placement=placement,
                           start=start,
                           end=end,
                           ordered_units=1000,
                           total_cost=100)
        flight_1_name = "Flight#1"
        flight_2_name = "Flight#2"
        Flight.objects.create(id="1", name=flight_1_name, **flight_data)
        Flight.objects.create(id="2", name=flight_2_name, **flight_data)
        account = Account.objects.create(pk="1", name="")
        stats = dict(cost=.5, video_views=10)
        campaign = Campaign.objects.create(pk="1", name="", account=account,
                                           salesforce_placement=placement,
                                           **stats)
        CampaignStatistic.objects.create(campaign=campaign, date=start,
                                         **stats)
        recalculate_de_norm_fields_for_account(account.id)

        send_daily_email_reports(reports=["CampaignUnderPacing"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].subject,
                         "FLIGHT UNDER PACING for {}".format(opportunity.name))
        self.assertEqual(mail.outbox[0].to, [ad_ops.email])
        self.assertEqual(mail.outbox[0].from_email, settings.EXPORTS_EMAIL_ADDRESS)
        self.assertEqual(set(mail.outbox[0].cc), set([acc_mng.email] + settings.PACING_REPORT_EMAIL_RECIPIENTS))
        body = mail.outbox[0].body
        body_template = "The flight {} is under pacing by"
        self.assertIn(body_template.format(flight_1_name), body)
        self.assertIn(body_template.format(flight_2_name), body)
