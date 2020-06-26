from datetime import datetime
from datetime import time
from datetime import timedelta

import pytz
from django.utils import timezone

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import FlightStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import Alert
from aw_reporting.models.salesforce_constants import FlightAlert
from aw_reporting.models.salesforce_constants import PlacementAlert
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
from aw_reporting.utils import get_dates_range
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class PacingReportTestCase(ExtendedAPITestCase):

    def test_period_dates_this_month(self):
        report = PacingReport()
        report.today = datetime(2016, 3, 13, tzinfo=pytz.utc).date()
        start, end = report.get_period_dates("this_month", None, None)
        self.assertEqual(start, datetime(2016, 3, 1, tzinfo=pytz.utc).date())
        self.assertEqual(end, datetime(2016, 3, 31, tzinfo=pytz.utc).date())

    def test_period_dates_next_month(self):
        report = PacingReport()
        report.today = datetime(1977, 12, 1, tzinfo=pytz.utc).date()
        start, end = report.get_period_dates("next_month", None, None)
        self.assertEqual(start, datetime(1978, 1, 1, tzinfo=pytz.utc).date())
        self.assertEqual(end, datetime(1978, 1, 31, tzinfo=pytz.utc).date())

    def test_period_dates_this_quarter(self):
        report = PacingReport()
        report.today = datetime(1955, 6, 30, tzinfo=pytz.utc).date()
        start, end = report.get_period_dates("this_quarter", None, None)
        self.assertEqual(start, datetime(1955, 4, 1, tzinfo=pytz.utc).date())
        self.assertEqual(end, datetime(1955, 6, 30, tzinfo=pytz.utc).date())

    def test_period_dates_next_quarter(self):
        report = PacingReport()
        report.today = datetime(1955, 6, 30, tzinfo=pytz.utc).date()
        start, end = report.get_period_dates("next_quarter", None, None)
        self.assertEqual(start, datetime(1955, 7, 1, tzinfo=pytz.utc).date())
        self.assertEqual(end, datetime(1955, 9, 30, tzinfo=pytz.utc).date())

    def test_period_dates_this_year(self):
        report = PacingReport()
        report.today = datetime(1955, 6, 30, tzinfo=pytz.utc).date()
        start, end = report.get_period_dates("this_year", None, None)
        self.assertEqual(start, datetime(1955, 1, 1, tzinfo=pytz.utc).date())
        self.assertEqual(end, datetime(1955, 12, 31, tzinfo=pytz.utc).date())

    def test_period_dates_next_year(self):
        report = PacingReport()
        report.today = datetime(2016, 1, 1, tzinfo=pytz.utc).date()
        start, end = report.get_period_dates("next_year", None, None)
        self.assertEqual(start, datetime(2017, 1, 1, tzinfo=pytz.utc).date())
        self.assertEqual(end, datetime(2017, 12, 31, tzinfo=pytz.utc).date())

    def test_ended_opportunity(self):
        today = datetime.now()
        tz = "UTC"
        last_update = datetime.combine(today, time.min).replace(tzinfo=pytz.timezone(tz))
        start = today - timedelta(days=3)
        end = today - timedelta(days=2)
        self.create_test_user()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end, probability=100
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        Flight.objects.create(
            id="1", name="", placement=placement, start=start, end=end, total_cost=10, ordered_units=100,
        )
        account = Account.objects.create(timezone=tz, update_time=last_update)
        campaign = Campaign.objects.create(
            id="1", account=account, name="", salesforce_placement=placement, video_views=102,
        )
        CampaignStatistic.objects.create(date=start, campaign=campaign, video_views=102)
        recalculate_de_norm_fields_for_account(account.id)

        report = PacingReport()
        opportunities = report.get_opportunities(dict(period="custom", start=start, end=end))
        self.assertEqual(len(opportunities), 1)
        opportunity_data = opportunities[0]
        self.assertEqual(opportunity_data["pacing"], 1)  # 100%

        placements = report.get_placements(opportunity)
        self.assertEqual(len(placements), 1)
        placement_data = placements[0]
        self.assertEqual(placement_data["pacing"], 1)

    def test_client_cost_and_over_delivered_opportunity(self):
        """
        Margin
        Apply If/Else logic: IF actual spend is below contracted,
        use actual. Else, use contracted budget
        :return:
        """
        today = datetime.now()
        start = today - timedelta(days=3)
        end = today - timedelta(days=2)
        self.create_test_user()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end, probability=100,
            budget=10  # margin will be 33%
        )

        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end, ordered_rate=.1,
            total_cost=10,  # plan cost
        )
        Flight.objects.create(
            id="1", name="", placement=placement,
            total_cost=10, ordered_units=100, start=start, end=end,
        )
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            account=Account.objects.create(id=next(int_iterator)),
            video_views=204,
            cost=10.2,
        )
        # 204 = x2 ordered_units - over delivery
        CampaignStatistic.objects.create(campaign=campaign, date=start, video_views=204, cost=10.2, )
        recalculate_de_norm_fields_for_account(campaign.account_id)

        report = PacingReport()
        opportunities = report.get_opportunities(dict(period="custom", start=start, end=end))
        self.assertEqual(len(opportunities), 1)

        opportunity_data = opportunities[0]
        self.assertAlmostEqual(opportunity_data["margin"], -0.02, 10)

        placements = report.get_placements(opportunity)
        self.assertEqual(len(placements), 1)
        placement_data = placements[0]
        self.assertAlmostEqual(
            placement_data["margin"], -0.02, places=10,
            msg="The margin is going to be -2%, because total_cost=10 "
                "and actual cost=10.2 ",
        )

    def test_list_global_visibility_on(self):
        """
        If global visibility at the Admin tab is enabled
        Items that matched with visible accounts are shown
        :return:
        """
        today = timezone.now()
        opportunity1 = Opportunity.objects.create(
            id="1", name="", start=today, end=today, probability=100
        )
        placement1 = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity1,
        )
        account1 = Account.objects.create(id="1", name="Visible Account")
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement1, account=account1,
        )
        opportunity2 = Opportunity.objects.create(
            id="2", name="", start=today, end=today, probability=100
        )
        placement2 = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity2,
        )
        account2 = Account.objects.create(id="2", name="Invisible Account")
        Campaign.objects.create(
            id="2", name="", salesforce_placement=placement2, account=account2,
        )

        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
            UserSettingsKey.VISIBLE_ACCOUNTS: [account1.id]
        }
        user = self.create_test_user()
        user.aw_settings.update(**user_settings)
        report = PacingReport()
        opportunities = report.get_opportunities(dict(), user=user)
        self.assertEqual(len(opportunities), 1)
        opportunity_data = opportunities[0]
        self.assertEqual(opportunity_data["id"], opportunity1.id)

    def test_flight_delivered_all(self):
        today = now_in_default_tz().date()
        start = today - timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=today,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=100,
            total_cost=10, start=start, end=today,
        )
        account = Account.objects.create(id="1", name="Visible Account")
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement, account=account,
        )
        CampaignStatistic.objects.create(
            campaign=campaign, date=start, video_views=102, cost=51,
        )

        report = PacingReport()

        flights = report.get_flights(placement)
        self.assertEqual(len(flights), 1)
        self.assertEqual(flights[0]["today_budget"], 0)

        campaigns = report.get_campaigns(flight)
        self.assertEqual(len(campaigns), 1)
        self.assertEqual(campaigns[0]["today_budget"], 0)

    def test_view_rate_color_scale(self):
        """
        View Rate: pull from Adwords “View Rate”, apply color scale 0-20%, 20-30%, 30%+
        :return:
        """
        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=today - timedelta(days=1), end=today + timedelta(days=1),
        )
        placement = OpPlacement.objects.create(
            id="1",
            name="",
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
        )
        flight = Flight.objects.create(id="1", name="", placement=placement, start=today, end=today)
        account = Account.objects.create(id="1", name="")
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement, account=account, video_views=1
        )
        CampaignStatistic.objects.create(campaign=campaign, date=today, video_views=1, impressions=10)
        CampaignStatistic.objects.create(campaign=campaign, date=today, device_id=1, video_views=0, impressions=10)
        recalculate_de_norm_fields_for_account(account.id)

        report = PacingReport()

        # low rate
        campaigns = report.get_campaigns(flight)
        self.assertEqual(len(campaigns), 1)
        self.assertEqual(campaigns[0]["video_view_rate"], 0.05)
        self.assertEqual(campaigns[0]["video_view_rate_quality"], 0)

        # normal rate
        CampaignStatistic.objects.create(campaign=campaign, date=today, device_id=2, video_views=24, impressions=80)
        recalculate_de_norm_fields_for_account(account.id)
        campaigns = report.get_campaigns(flight)
        self.assertEqual(len(campaigns), 1)
        self.assertEqual(campaigns[0]["video_view_rate"], 0.25)
        self.assertEqual(campaigns[0]["video_view_rate_quality"], 1)

        # high rate
        CampaignStatistic.objects.create(campaign=campaign, date=today, device_id=3, video_views=5, impressions=0)
        recalculate_de_norm_fields_for_account(account.id)
        campaigns = report.get_campaigns(flight)
        self.assertEqual(len(campaigns), 1)
        self.assertEqual(campaigns[0]["video_view_rate"], 0.30)
        self.assertEqual(campaigns[0]["video_view_rate_quality"], 2)

    def test_ctr_color_scale(self):
        """
        CTR:  clicks / impressions. 0-0.5%, 0.5-0.75%, 0.75%+
        :return:
        """
        today = timezone.now().date()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=today - timedelta(days=1), end=today + timedelta(days=1),
        )
        placement = OpPlacement.objects.create(
            id="1",
            name="",
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
        )
        flight = Flight.objects.create(id="1", name="", placement=placement, start=today, end=today)
        account = Account.objects.create(id="1", name="")
        campaign = Campaign.objects.create(id="1", name="", salesforce_placement=placement, account=account)
        CampaignStatistic.objects.create(campaign=campaign, date=today, clicks=1, impressions=400)
        recalculate_de_norm_fields_for_account(account.id)

        report = PacingReport()

        # low rate
        campaigns = report.get_campaigns(flight)
        self.assertEqual(len(campaigns), 1)
        self.assertEqual(campaigns[0]["ctr"], 0.0025)
        self.assertEqual(campaigns[0]["ctr_quality"], 0)

        # normal rate
        CampaignStatistic.objects.create(campaign=campaign, date=today, device_id=2, clicks=1)
        recalculate_de_norm_fields_for_account(account.id)
        campaigns = report.get_campaigns(flight)
        self.assertEqual(len(campaigns), 1)
        self.assertEqual(campaigns[0]["ctr"], 0.005)
        self.assertEqual(campaigns[0]["ctr_quality"], 1)

        # high rate
        CampaignStatistic.objects.create(campaign=campaign, date=today, device_id=3, clicks=1)
        recalculate_de_norm_fields_for_account(account.id)
        campaigns = report.get_campaigns(flight)
        self.assertEqual(len(campaigns), 1)
        self.assertEqual(campaigns[0]["ctr"], 0.0075)
        self.assertEqual(campaigns[0]["ctr_quality"], 2)

    def test_opportunity_pacing_calculation(self):
        """
        pacing must be calculated based on daily plan at the flight level
        :return:
        """
        today = now_in_default_tz().date()
        tz = "UTC"
        last_update = datetime.combine(today, time.min).replace(tzinfo=pytz.timezone(tz))
        yesterday = today - timedelta(days=1)
        self.create_test_user()
        opportunity = Opportunity.objects.create(id="1", name="", start=today, end=today, probability=100)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,  # CPV
        )
        Flight.objects.create(
            id="1", name="100 units/day and 500units up today", placement=placement,
            start=yesterday - timedelta(days=4), end=yesterday - timedelta(days=3), ordered_units=300,
        )
        Flight.objects.create(
            id="2", name="200 units/day and 200units up today", placement=placement,
            start=yesterday - timedelta(days=1), end=yesterday + timedelta(days=8), ordered_units=2000,
        )

        account = Account.objects.create(timezone=tz, update_time=last_update)
        campaign = Campaign.objects.create(id="1", account=account,
                                           salesforce_placement=placement, video_views=700)
        CampaignStatistic.objects.create(date=yesterday, campaign=campaign, video_views=700)
        recalculate_de_norm_fields_for_account(account.id)

        report = PacingReport()
        opportunities = report.get_opportunities({})
        self.assertEqual(len(opportunities), 1)
        opportunity = opportunities[0]

        # 700 + 2% ordered by yesterday and 700 delivered views = 100% - 2% = 98%
        self.assertAlmostEqual(opportunity["pacing"], .98, places=3)

    def test_placement_pacing_calculation(self):
        """
        pacing must be calculated based on daily plan at the flight level
        :return:
        """
        today = now_in_default_tz().date()
        tz = "UTC"
        last_update = datetime.combine(today, time.min).replace(tzinfo=pytz.timezone(tz))
        yesterday = today - timedelta(days=1)
        opportunity = Opportunity.objects.create(id="1", name="", start=today, end=today, probability=100)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        Flight.objects.create(
            id="1", name="100 units/day and 500units up today", placement=placement,
            start=yesterday - timedelta(days=4), end=yesterday - timedelta(days=3), ordered_units=300,
        )
        Flight.objects.create(
            id="2", name="200 units/day and 200units up today", placement=placement,
            start=yesterday - timedelta(days=1), end=yesterday + timedelta(days=8), ordered_units=2000,
        )

        account = Account.objects.create(timezone=tz, update_time=last_update)
        campaign = Campaign.objects.create(id="1", salesforce_placement=placement, video_views=700,
                                           account=account)
        CampaignStatistic.objects.create(date=yesterday, campaign=campaign, video_views=700)
        recalculate_de_norm_fields_for_account(account.id)

        report = PacingReport()
        placements = report.get_placements(opportunity)
        self.assertEqual(len(placements), 1)
        placement = placements[0]

        # 700 + 2% ordered by yesterday and 700 delivered views = 100% - 2% = 98%
        self.assertAlmostEqual(placement["pacing"], .98, places=3)

    def test_pacing_report_flight_historical_goal_charts(self):
        today = now_in_default_tz().date()
        start = today - timedelta(days=4)
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=today,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=50, cost=50,
            total_cost=50, start=start, end=today, delivered=50
        )
        FlightStatistic.objects.create(flight=flight, delivery=flight.ordered_units,  impressions=50, sum_cost=50)
        account = Account.objects.create(id="1", name="Visible Account")
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement, account=account,
        )
        stats = []
        for date in get_dates_range(flight.start, flight.end):
            stats.append(CampaignStatistic(
                campaign=campaign, date=date, impressions=10, cost=10,
            ))
        CampaignStatistic.objects.bulk_create(stats)
        report = PacingReport()
        flight = report.get_flights(placement)[0]
        goal_units = []
        actual_units = []
        goal_spend = []
        actual_spend = []
        for item in flight["historical_units_chart"]["data"]:
            goal_units.append(item["goal"])
            actual_units.append(item["actual"])
        for item in flight["historical_spend_chart"]["data"]:
            goal_spend.append(item["goal"])
            actual_spend.append(item["actual"])
        self.assertEqual(set(actual_units), set(stat.impressions for stat in stats))
        self.assertEqual(set(actual_spend), set(stat.cost for stat in stats))
        self.assertEqual(set(goal_units), {10})
        self.assertEqual(set(goal_spend), {10})

    def test_alert_signals(self):
        today = now_in_default_tz().date()

        start = today - timedelta(days=4)
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=today,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            ordered_units=50
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, ordered_units=50, cost=50,
            total_cost=50, start=start, end=today, delivered=0,
        )
        self.assertFalse(Alert.objects.all().exists())

        placement.ordered_units *= 2
        placement.save()
        placement_ordered_units_changed = Alert.objects.filter(
            record_id=placement.id, code=PlacementAlert.ORDERED_UNITS_CHANGED.value)
        self.assertTrue(placement_ordered_units_changed.exists())

        flight.end += timedelta(days=1)
        flight.save()

        flight_date_changed_alert = Alert.objects.filter(record_id=flight.id, code=FlightAlert.DATES_CHANGED.value)
        self.assertTrue(flight_date_changed_alert.exists())

    def test_alert_opportunity_under_margin(self):
        today = datetime.now()
        tz = "UTC"
        last_update = datetime.combine(today, time.min).replace(tzinfo=pytz.timezone(tz))
        start = today - timedelta(days=10)
        end = today + timedelta(days=10)
        self.create_test_user()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end, probability=100
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        Flight.objects.create(
            id="1", name="", placement=placement, start=start, end=end, total_cost=1000, ordered_units=100,
        )
        account = Account.objects.create(timezone=tz, update_time=last_update)
        campaign = Campaign.objects.create(
            id="1", account=account, name="", salesforce_placement=placement, video_views=102,
        )
        CampaignStatistic.objects.create(date=start, campaign=campaign, video_views=82)
        recalculate_de_norm_fields_for_account(account.id)

        report = PacingReport()
        opportunity = report.get_opportunities(dict())[0]
        opportunity_alerts = " ".join(opportunity["alerts"])
        self.assertTrue("is under margin", opportunity_alerts)

    def test_alert_flight_80_delivery(self):
        today = datetime.now()
        tz = "UTC"
        last_update = datetime.combine(today, time.min).replace(tzinfo=pytz.timezone(tz))
        start = today - timedelta(days=10)
        end = today - timedelta(days=2)
        self.create_test_user()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end, probability=100
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        Flight.objects.create(
            id="1", name="", placement=placement, start=start, end=end, total_cost=10, ordered_units=100,
        )
        account = Account.objects.create(timezone=tz, update_time=last_update)
        campaign = Campaign.objects.create(
            id="1", account=account, name="", salesforce_placement=placement, video_views=102,
        )
        CampaignStatistic.objects.create(date=start, campaign=campaign, video_views=82)
        recalculate_de_norm_fields_for_account(account.id)

        report = PacingReport()

        flight = report.get_flights(placement)[0]
        flight_alerts = " ".join(flight["alerts"])
        self.assertTrue("delivered 80%" in flight_alerts)

    def test_alert_flight_100_delivery(self):
        today = datetime.now()
        tz = "UTC"
        last_update = datetime.combine(today, time.min).replace(tzinfo=pytz.timezone(tz))
        start = today - timedelta(days=3)
        end = today - timedelta(days=2)
        self.create_test_user()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end, probability=100
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        Flight.objects.create(
            id="1", name="", placement=placement, start=start, end=end, total_cost=10, ordered_units=100,
        )
        account = Account.objects.create(timezone=tz, update_time=last_update)
        campaign = Campaign.objects.create(
            id="1", account=account, name="", salesforce_placement=placement, video_views=102,
        )
        CampaignStatistic.objects.create(date=start, campaign=campaign, video_views=102)
        recalculate_de_norm_fields_for_account(account.id)

        report = PacingReport()
        flight = report.get_flights(placement)[0]
        flight_alerts = " ".join(flight["alerts"])
        self.assertTrue("delivered 100%" in flight_alerts)

    def test_alert_flight_dates_changed(self):
        today = datetime.now()
        start = today - timedelta(days=3)
        end = today
        self.create_test_user()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end, probability=100
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        flight = Flight.objects.create(
            id="1", name="", placement=placement, start=start, end=end, total_cost=10, ordered_units=100,
        )
        flight.end += timedelta(days=1)
        flight.save()

        report = PacingReport()
        flight = report.get_flights(placement)[0]
        flight_alerts = " ".join(flight["alerts"])
        self.assertTrue("dates have been changed", flight_alerts)

    def test_alert_placement_ordered_units_changed(self):
        today = datetime.now()
        start = today - timedelta(days=3)
        end = today
        self.create_test_user()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end, probability=100
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity, ordered_units=100,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        Flight.objects.create(
            id="1", name="", placement=placement, start=start, end=end, total_cost=10, ordered_units=100,
        )
        placement.ordered_units *= 2
        placement.save()

        report = PacingReport()
        placement = report.get_placements(opportunity)[0]
        placement_alerts = " ".join(placement["alerts"])
        self.assertTrue("ordered units have been changed", placement_alerts)
