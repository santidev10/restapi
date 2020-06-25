from datetime import datetime
from datetime import timedelta

from django.core import mail
from django.test import override_settings

from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import CampaignStatus
from aw_reporting.models import Flight
from aw_reporting.models import FlightStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import User
from email_reports.tasks import send_daily_email_reports
from utils.unittests.patch_now import patch_now
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase


class FlightDeliveredEmailsTestCase(APITestCase):
    def test_flight_alerts(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        ordered_views = 1000
        test_cost_1, test_views_1 = 123, 500
        test_cost_2, test_views_2 = 1240, 480
        days_left = 3

        now = datetime(2017, 1, 1)
        today = now.date()
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id="1",
            name="Placement",
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(id="1", name="Flight", placement=placement,
                                       start=today - timedelta(days=10),
                                       end=today + timedelta(days=days_left - 1),
                                       ordered_units=ordered_views)

        FlightStatistic.objects.create(flight=flight, video_views=test_views_1 + test_views_2)

        campaign = Campaign.objects.create(pk="1", name="",
                                           salesforce_placement=placement,
                                           status=CampaignStatus.SERVING.value)

        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=2),
                                         video_views=test_views_1,
                                         cost=test_cost_1)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=1),
                                         video_views=test_views_2,
                                         cost=test_cost_2)

        with patch_now(now), \
             override_settings(PACING_NOTIFICATIONS=["80"]):
            send_daily_email_reports(reports=["FlightDeliveredReport"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[-1]

        self.assertEqual(email.body, "Flight in Opportunity has delivered 80% of its ordered units")
        self.assertEqual(email.subject, "80% DELIVERY - Flight")

    def test_flight_alerts_not_sent_twice(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        ordered_views = 1000
        test_cost_1, test_views_1 = 123, 500
        test_cost_2, test_views_2 = 1240, 480
        days_left = 3

        now = datetime(2017, 1, 1)
        today = now.date()
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id="1",
            name="Placement",
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(id="1", name="Flight", placement=placement,
                                       start=today - timedelta(days=10),
                                       end=today + timedelta(days=days_left - 1),
                                       ordered_units=ordered_views)

        FlightStatistic.objects.create(flight=flight, video_views=test_views_1 + test_views_2)

        campaign = Campaign.objects.create(pk="1", name="",
                                           salesforce_placement=placement,
                                           status=CampaignStatus.SERVING.value)

        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=2),
                                         video_views=test_views_1,
                                         cost=test_cost_1)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=1),
                                         video_views=test_views_2,
                                         cost=test_cost_2)

        with patch_now(now), override_settings(PACING_NOTIFICATIONS=["80"]):
            send_daily_email_reports(reports=["FlightDeliveredReport"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[-1]

        self.assertEqual(email.body, "Flight in Opportunity has delivered 80% of its ordered units")
        self.assertEqual(email.subject, "80% DELIVERY - Flight")

        with patch_now(now):
            send_daily_email_reports(reports=["FlightDeliveredReport"], debug=False)

        self.assertEqual(len(mail.outbox), 1)

    def test_flight_alerts_100_delivered(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        ordered_views = 1000
        test_cost_1, test_views_1 = 123, 540
        test_cost_2, test_views_2 = 1240, 480
        days_left = 3

        now = datetime(2017, 1, 1)
        today = now.date()
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id="1",
            name="Placement",
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(id="1", name="Flight", placement=placement,
                                       start=today - timedelta(days=10),
                                       end=today + timedelta(days=days_left - 1),
                                       ordered_units=ordered_views)

        FlightStatistic.objects.create(flight=flight, video_views=test_views_1 + test_views_2)
        campaign = Campaign.objects.create(pk="1", name="",
                                           salesforce_placement=placement,
                                           status=CampaignStatus.SERVING.value)

        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=2),
                                         video_views=test_views_1,
                                         cost=test_cost_1)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=1),
                                         video_views=test_views_2,
                                         cost=test_cost_2)

        with patch_now(now):
            send_daily_email_reports(reports=["FlightDeliveredReport"], debug=False)

        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[-1]

        self.assertEqual(email.body, "Flight in Opportunity has delivered 100% of its ordered units")
        self.assertEqual(email.subject, "100% DELIVERY - Flight")

    def test_flight_alerts_not_sent_for_ended_campaign(self):
        ad_ops = User.objects.create(id="1", name="Paul", email="1@mail.cz")
        ordered_views = 1000
        test_cost_1, test_views_1 = 123, 540
        test_cost_2, test_views_2 = 1240, 480

        now = datetime(2017, 1, 1)
        today = now.date()
        opportunity = Opportunity.objects.create(
            id="solo", name="Opportunity",
            ad_ops_manager=ad_ops,
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id="1",
            name="Placement",
            start=today - timedelta(days=2),
            end=today + timedelta(days=2),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(id="1", name="Flight", placement=placement,
                                       start=today - timedelta(days=10),
                                       end=today - timedelta(days=1),
                                       ordered_units=ordered_views)

        FlightStatistic.objects.create(flight=flight, video_views=test_views_1 + test_views_2)
        campaign = Campaign.objects.create(pk="1", name="",
                                           salesforce_placement=placement,
                                           status=CampaignStatus.ENDED.value)

        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=2),
                                         video_views=test_views_1,
                                         cost=test_cost_1)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today - timedelta(days=1),
                                         video_views=test_views_2,
                                         cost=test_cost_2)

        with patch_now(now):
            send_daily_email_reports(reports=["FlightDeliveredReport"], debug=False)

        self.assertEqual(len(mail.outbox), 0)
