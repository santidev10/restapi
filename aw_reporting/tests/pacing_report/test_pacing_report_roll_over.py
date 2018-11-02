from datetime import timedelta, datetime

from django.utils import timezone

from aw_reporting.models import Opportunity, OpPlacement, SalesForceGoalType, \
    Flight, Account, Campaign, CampaignStatistic
from aw_reporting.reports.pacing_report import PacingReport
from utils.datetime import now_in_default_tz
from utils.utils_tests import ExtendedAPITestCase


class PacingReportTestCase(ExtendedAPITestCase):

    def test_cary_over_disabled_no_over_delivery(self):
        today = timezone.now().date()
        start = today - timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id='1', name="", start=start, end=today,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=1000,
            total_cost=10, start=start, end=today,
        )
        account = Account.objects.create(id="1", name="Visible Account")
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement, account=account,
        )
        report = PacingReport()

        # without over delivery
        # test flight
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)
        expected_goal_views = flight.ordered_units * 1.02
        self.assertEqual(flights[0]['plan_video_views'], expected_goal_views)

        first_chart = flights[0]['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(first_chart['data'][-1]['value'], expected_goal_views)

        # test campaign
        campaigns = report.get_campaigns(flight)
        self.assertEqual(campaigns[0]['plan_video_views'], expected_goal_views)
        first_chart = campaigns[0]['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(first_chart['data'][-1]['value'], expected_goal_views)

    def test_cary_over_disabled_over_delivery(self):
        today = timezone.now().date()
        start = today - timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id='1', name="", start=start, end=today,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
        )
        prev_end = start - timedelta(days=2)
        prev_start = prev_end - timedelta(days=2)
        Flight.objects.create(
            id="0", name="", placement=placement, ordered_units=500,
            total_cost=10, start=prev_start, end=prev_end,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=1000,
            total_cost=10, start=start, end=today,
        )
        account = Account.objects.create(id="1", name="Visible Account")
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement, account=account,
        )
        # with over delivery
        CampaignStatistic.objects.create(
            date=prev_end, campaign=campaign, video_views=1530,  # 1020 over delivery
        )
        report = PacingReport()
        # test flight
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 2)

        flight_data = flights[1]
        self.assertEqual(flight_data['plan_video_views'], 0)  # 1020 - 1020
        first_chart = flight_data['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(first_chart['data'][-1]['value'], 0)

        # test campaign
        campaigns = report.get_campaigns(flight)
        self.assertEqual(campaigns[0]['plan_video_views'], 0)
        first_chart = campaigns[0]['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(first_chart['data'][-1]['value'], 0)

    def test_cary_over_disabled_over_delivery_and_pre_flight(self):
        today = timezone.now().date()
        start = today - timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id='1', name="", start=start, end=today,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=1000,
            total_cost=10, start=start, end=today,
        )
        Flight.objects.create(
            id=flight.id * 3,
            name="Flight Name",
            start=flight.start - timedelta(days=20),
            end=flight.start - timedelta(days=1),
            placement=placement,
            ordered_units=400,
        )
        account = Account.objects.create(id="1", name="Visible Account")
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement, account=account,
        )
        CampaignStatistic.objects.create(
            date=flight.start - timedelta(days=1),
            campaign=campaign, video_views=1020,  # over delivery
        )

        report = PacingReport()

        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 2)
        self.assertEqual(flights[1]['plan_video_views'], 408)
        first_chart = flights[1]['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(
            first_chart['data'][-1]['value'],
            408,  # 400 + 2%
            "Over-delivery = 600 and ordered_units = 1000, "
            "so plan for this flight = 400"
        )

        # test campaign
        campaigns = report.get_campaigns(flight)
        self.assertEqual(campaigns[0]['plan_video_views'], 408)
        first_chart = campaigns[0]['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(first_chart['data'][-1]['value'], 408)

    def test_cary_over_disabled_with_flights_before_and_after(self):
        """
        there is an amount of delivered units in the previous month - 1020
        there is a flight in the previous month that consumes 400 of them
        there are two flights that divide between themselves these 600 units based on counts of days they have
        good luck
        :return:
        """
        today = timezone.now().date()
        start = today - timedelta(days=19)
        opportunity = Opportunity.objects.create(
            id='1', name="", start=start, end=today,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(
            id="1", name="Current Flight", placement=placement, ordered_units=1000,
            total_cost=10, start=start, end=today,
        )
        account = Account.objects.create(id="1", name="Visible Account")
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement, account=account,
        )
        CampaignStatistic.objects.create(
            date=flight.start - timedelta(days=1),
            campaign=campaign, video_views=1020,  # over delivery
        )
        # with over delivery and the previous flight
        Flight.objects.create(
            id=flight.id * 3,
            name="Per Flight",
            start=flight.start - timedelta(days=20),
            end=flight.start - timedelta(days=1),
            placement=placement,
            ordered_units=400,
        )
        # and with the followed flight
        Flight.objects.create(
            id=flight.id * 4,
            name="After Flight",
            start=flight.end + timedelta(days=1),
            end=flight.end + timedelta(days=5),  # 5 days
            placement=placement,
            ordered_units=0,  # this doesn't matter
        )

        report = PacingReport()
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 3)

        flight_data = flights[1]
        self.assertEqual(flight_data['id'], flight.id)
        first_chart = flight_data['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(
            first_chart['data'][-1]['value'],
            530.4,  # 520 + 2%
            "Over-delivery = 600 and "
            "total planned days number for two flights = 25"
            "so 600 / 25 = 24 over-delivered units per day"
            "and whole over-delivery for this flight is 24 * 20 days = 480"
            "finally planned is 1000 - 480 = 520"
        )

        # test campaign
        campaigns = report.get_campaigns(flight)
        self.assertEqual(campaigns[0]['plan_video_views'], 530.4)
        first_chart = campaigns[0]['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(first_chart['data'][-1]['value'], 530.4)

    def test_cary_over_disabled_with_ended_over_and_under_delivered_flights(self):
        """
        CASE:
        today = March 1st
        Flight January: initial_plan=1020, delivered=2020, shown_plan=1020, over_delivery=1000
        Flight February: initial_plan=1020, delivered=520, shown_plan=520, over_delivery=500
        Flight March: initial_plan=1020, delivered=0, shown_plan=770, over_delivery=250
        Flight April: initial_plan=1020, delivered=0, shown_plan=770, over_delivery=0
        :return:
        """
        today = datetime(2017, 3, 1).date()
        opportunity = Opportunity.objects.create(id='1', name="", start=today, end=today)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
        )
        campaign = Campaign.objects.create(id="1", name="", salesforce_placement=placement)
        jan_start = today.replace(month=1)
        jan_end = jan_start.replace(day=31)
        jan = Flight.objects.create(id="1", name="", placement=placement, ordered_units=1000,
                                    start=jan_start, end=jan_end)
        CampaignStatistic.objects.create(date=jan_start, campaign=campaign, video_views=1010)
        CampaignStatistic.objects.create(date=jan_end, campaign=campaign, video_views=1010)

        feb_start = today.replace(month=2)
        feb_end = feb_start.replace(day=28)
        feb = Flight.objects.create(id="2", name="", placement=placement, ordered_units=1000,
                                    start=feb_start, end=feb_end)
        CampaignStatistic.objects.create(date=feb_start + timedelta(days=10), campaign=campaign, video_views=220)
        CampaignStatistic.objects.create(date=feb_end, campaign=campaign, video_views=300)

        march_start = today.replace(month=3)
        march_end = march_start.replace(day=30)
        march = Flight.objects.create(id="3", name="", placement=placement, ordered_units=1000,
                                      start=march_start, end=march_end)

        april_start = today.replace(month=4)
        april_end = april_start.replace(day=30)
        april = Flight.objects.create(id="4", name="", placement=placement, ordered_units=1000,
                                      start=april_start, end=april_end)

        # check
        report = PacingReport(today=today)
        flights = report.get_flights(placement)

        self.assertEqual(len(flights), 4)

        flight_1 = flights[0]
        self.assertEqual(flight_1['id'], jan.id)
        self.assertEqual(flight_1['plan_video_views'], 1020)
        pacing_chart = flight_1['charts'][0]
        self.assertEqual(pacing_chart['title'], 'Ideal Pacing')
        self.assertEqual(
            pacing_chart['data'][-1]['value'], 1020,  # 1000 + 2%
        )

        flight_2 = flights[1]
        self.assertEqual(flight_2['id'], feb.id)
        self.assertEqual(flight_2['plan_video_views'], 520)
        pacing_chart = flight_2['charts'][0]
        self.assertEqual(pacing_chart['title'], 'Ideal Pacing')
        self.assertEqual(
            pacing_chart['data'][-1]['value'], 520,
        )

        flight_3 = flights[2]
        self.assertEqual(flight_3['id'], march.id)
        self.assertEqual(flight_3['plan_video_views'], 770)
        pacing_chart = flight_3['charts'][0]
        self.assertEqual(pacing_chart['title'], 'Ideal Pacing')
        self.assertAlmostEqual(pacing_chart['data'][-1]['value'], 770, places=10)

        flight_4 = flights[3]
        self.assertEqual(flight_4['id'], april.id)
        self.assertEqual(flight_4['plan_video_views'], 770)
        pacing_chart = flight_4['charts'][0]
        self.assertEqual(pacing_chart['title'], 'Ideal Pacing')
        self.assertAlmostEqual(pacing_chart['data'][-1]['value'], 770)

        # test campaigns
        campaigns = report.get_campaigns(jan)
        self.assertEqual(campaigns[0]['plan_video_views'], 1020)
        first_chart = campaigns[0]['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(first_chart['data'][-1]['value'], 1020)

        campaigns = report.get_campaigns(feb)
        self.assertEqual(campaigns[0]['plan_video_views'], 520)
        first_chart = campaigns[0]['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(first_chart['data'][-1]['value'], 520)

        campaigns = report.get_campaigns(march)
        self.assertEqual(campaigns[0]['plan_video_views'], 770)
        first_chart = campaigns[0]['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertAlmostEqual(first_chart['data'][-1]['value'], 770)

        campaigns = report.get_campaigns(april)
        self.assertEqual(campaigns[0]['plan_video_views'], 770)
        first_chart = campaigns[0]['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertAlmostEqual(first_chart['data'][-1]['value'], 770)

    def test_cary_over_enabled(self):
        today = now_in_default_tz().date()
        start = today - timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id='1', name="", start=start, end=today,
            cannot_roll_over=True,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=1000,
            total_cost=10, start=start, end=today,
        )
        account = Account.objects.create(id="1", name="")
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement, account=account,
        )
        CampaignStatistic.objects.create(
            campaign=campaign, date=start, video_views=1000, cost=51,
        )

        report = PacingReport()
        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)

        flight_data = flights[0]
        self.assertEqual(flight_data['plan_video_views'], 1020)
        first_chart = flight_data['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')

        self.assertEqual(
            first_chart['data'][-1]['value'],
            1020,  # 1000 + 2%
            "Still 1000 because cannot_roll_over is True"
        )

        campaigns = report.get_campaigns(flight)
        campaign_data = campaigns[0]
        self.assertEqual(campaign_data['plan_video_views'], 1020)
        first_chart = campaign_data['charts'][0]
        self.assertEqual(first_chart['title'], 'Ideal Pacing')
        self.assertEqual(
            first_chart['data'][-1]['value'],
            1020,  # 1000 + 2%
            "Still 1000 because cannot_roll_over is True"
        )
