from datetime import timedelta

from django.core import mail
from django.core.management import call_command
from django.utils import timezone

from aw_reporting.models import SalesForceGoalType, User, Opportunity, \
    OpPlacement, Flight, Account, Campaign, CampaignStatistic
from utils.utils_tests import ExtendedAPITestCase as APITestCase


class SendDailyEmailsTestCase(APITestCase):

    def setUp(self):
        pass

    def test_send_minimum_email(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        sales = User.objects.create(id="2", name="Dave", email="2@mail.cz")
        acc_mng = User.objects.create(id="3", name="John", email="3@mail.cz")
        today = timezone.now().date()
        start, end = today - timedelta(days=2), today + timedelta(days=2)

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
        flight = Flight.objects.create(id="1", name="",
                                       placement=placement,
                                       start=start,
                                       end=end,
                                       ordered_units=1000,
                                       total_cost=100)
        account = Account.objects.create(pk="1", name="")
        stats = dict(cost=130.5, video_views=1210)
        campaign = Campaign.objects.create(pk="1", name="", account=account,
                                           salesforce_placement=placement,
                                           **stats)
        CampaignStatistic.objects.create(campaign=campaign, date=flight.start,
                                         **stats)

        call_command("send_daily_email_reports", reports="CampaignOverPacing")

        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].subject,
                         "FLIGHT OVER PACING for {}".format(opportunity.name))
        self.assertEqual(len(mail.outbox[0].to), 1)
        self.assertEqual(mail.outbox[0].to[0], ad_ops.email)
        self.assertEqual(len(mail.outbox[0].cc), 3)
