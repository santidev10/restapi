import logging
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from itertools import product
from urllib.parse import urlencode

import pytz
from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from django.db.models import Sum
from django.http import QueryDict
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Category
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import User
from aw_reporting.models.salesforce_constants import DYNAMIC_PLACEMENT_TYPES
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.reports.pacing_report import PacingReportChartId
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.utittests.generic_test import generic_test
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase

logger = logging.getLogger(__name__)


class PacingReportOpportunitiesTestCase(APITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.OPPORTUNITIES)

    def setUp(self):
        self.user = self.create_test_user()

    def test_forbidden_get_opportunities(self):
        self.user.delete()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_get_opportunities(self):
        today = timezone.now()
        month_ago, month_after = today - timedelta(days=31), today + timedelta(
            days=31)
        ad_ops_manager = User.objects.create(id="11", name="",
                                             email="my.email@mail.kz")
        ad_ops_user = get_user_model().objects.create(
            email=ad_ops_manager.email,
            profile_image_url="https://static.folder/very-ugly-photo.jpeg")
        category = Category.objects.create(id="Fun & despise")
        notes = "Ops it's a mistake"
        territory = "Test territory"
        current_op = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=1),
            end=today + timedelta(days=1),
            ad_ops_manager=ad_ops_manager,
            category=category, notes=notes,
            territory=territory,
            probability=100,
        )
        OpPlacement.objects.create(id="1", name="", opportunity=current_op,
                                   goal_type_id=SalesForceGoalType.CPM)
        OpPlacement.objects.create(id="2", name="", opportunity=current_op,
                                   goal_type_id=SalesForceGoalType.CPV)
        Opportunity.objects.create(id="2", name="2", start=month_ago,
                                   end=month_ago, probability=100)
        Opportunity.objects.create(id="3", name="3", start=month_after,
                                   end=month_after, probability=100)

        response = self.client.get("{}?period=this_month".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        item = data[0]
        self.assertEqual(
            set(item.keys()),
            {
                "ad_ops",
                "am",
                "apex_deal",
                "aw_update_time",
                "billing_server",
                "cannot_roll_over",
                "category",
                "chart_data",
                "cost",
                "cpm",
                "cpv",
                "cpm_buffer",
                "cpv_buffer",
                "ctr",
                "ctr_quality",
                "dynamic_placements_types",
                "end",
                "goal_type",
                "goal_type_ids",
                "has_dynamic_placements",
                "id",
                "impressions",
                "is_completed",
                "is_upcoming",
                "margin",
                "margin_cap_required",
                "margin_direction",
                "margin_quality",
                "name",
                "notes",
                "pacing",
                "pacing_direction",
                "pacing_quality",
                "plan_cost",
                "plan_cpm",
                "plan_cpv",
                "plan_impressions",
                "plan_video_views",
                "region",
                "sales",
                "start",
                "status",
                "thumbnail",
                "video_view_rate",
                "video_view_rate_quality",
                "video_views",
            }
        )
        self.assertEqual(item["id"], current_op.id)
        self.assertEqual(item['status'], "active")
        self.assertEqual(item['thumbnail'], ad_ops_user.profile_image_url)
        self.assertEqual(item['notes'], notes)
        self.assertEqual(item['category']['id'], category.id)
        self.assertEqual(item['category']['name'], category.name)
        self.assertEqual(item['region']['id'], territory)
        self.assertEqual(item['region']['name'], territory)

    def test_get_opportunities_buffers_default(self):
        today = timezone.now()
        first_day = today.replace(day=1)
        month_ago, month_after = first_day - timedelta(
            days=1), first_day + timedelta(days=32)

        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   probability=100)
        Opportunity.objects.create(id="2", name="2", start=month_ago,
                                   end=month_ago, probability=100)
        response = self.client.get((self.url))
        print(response.data)


    def test_get_opportunities_filter_period(self):
        today = timezone.now()
        first_day = today.replace(day=1)
        month_ago, month_after = first_day - timedelta(
            days=1), first_day + timedelta(days=32)

        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   probability=100)
        Opportunity.objects.create(id="2", name="2", start=month_ago,
                                   end=month_ago, probability=100)
        op_expected = Opportunity.objects.create(id="3", name="3",
                                                 start=month_after,
                                                 end=month_after,
                                                 probability=100)

        response = self.client.get("{}?period=next_month".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], op_expected.id)

    def test_get_opportunities_filter_ad_ops(self):
        user1 = User.objects.create(id="1", name="1")
        user2 = User.objects.create(id="2", name="2")
        today = timezone.now()
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   ad_ops_manager=user1, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="2",
                                                 start=today, end=today,
                                                 ad_ops_manager=user2,
                                                 probability=100)

        response = self.client.get("{}?ad_ops={}".format(self.url, user2.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], op_expected.id)

    def test_get_opportunities_filter_am(self):
        user1 = User.objects.create(id="1", name="1")
        user2 = User.objects.create(id="2", name="2")
        today = timezone.now()
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   account_manager=user1, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="2",
                                                 start=today, end=today,
                                                 account_manager=user2,
                                                 probability=100)

        response = self.client.get("{}?am={}".format(self.url, user2.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], op_expected.id)

    def test_get_opportunities_filter_sales(self):
        user1 = User.objects.create(id="1", name="1")
        user2 = User.objects.create(id="2", name="2")
        today = timezone.now()
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   sales_manager=user1, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="2", start=today,
                                                 end=today, sales_manager=user2,
                                                 probability=100)

        response = self.client.get("{}?sales={}".format(self.url, user2.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], op_expected.id)

    def test_get_opportunities_filter_category(self):
        category1 = Category.objects.create(id="Naive folks")
        category2 = Category.objects.create(id="Imagined bread")
        today = timezone.now()
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   category=category1, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="2", start=today,
                                                 end=today, category=category2,
                                                 probability=100)

        response = self.client.get(
            "{}?category={}".format(self.url, category2.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], op_expected.id)

    def test_get_opportunities_filter_goal(self):
        today = timezone.now()
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   goal_type_id=SalesForceGoalType.CPM,
                                   probability=100)
        op_expected = Opportunity.objects.create(
            id="2", name="2", start=today, end=today,
            goal_type_id=SalesForceGoalType.CPV, probability=100)

        response = self.client.get("{}?goal_type={}".format(self.url, 1))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], op_expected.id)

    def test_get_opportunities_filter_region(self):
        today = timezone.now()
        territory_1 = "Territory 1"
        territory_2 = "Territory 2"
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   territory=territory_1, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="2", start=today,
                                                 end=today, territory=territory_2,
                                                 probability=100)

        query = QueryDict(mutable=True)
        query.update(region=territory_2)
        response = self.client.get("{}?{}".format(self.url, urlencode(query)))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], op_expected.id)

    def test_get_opportunities_filter_region_multiple(self):
        today = timezone.now()
        territory_1 = "Territory 1"
        territory_2 = "Territory 2"
        Opportunity.objects.create(id="1", name="1",
                                   start=today, end=today,
                                   territory=territory_1, probability=100)
        Opportunity.objects.create(id="2", name="2",
                                   start=today, end=today,
                                   territory=territory_2, probability=100)

        query = QueryDict(mutable=True)
        query.update(region=territory_1)
        query.update(region=territory_2)
        response = self.client.get("{}?{}".format(self.url, query.urlencode()))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 2)

    def test_get_opportunities_filter_name(self):
        today = timezone.now()
        Opportunity.objects.create(id="1", name="Peter Griffin", start=today,
                                   end=today, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="Homer Simpson",
                                                 start=today, end=today,
                                                 probability=100)

        response = self.client.get("{}?search={}".format(self.url, "SIM"))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], op_expected.id)

    def test_get_opportunities_filter_status(self):
        today = timezone.now().date()
        month_ago, month_after = today - timedelta(days=31), today + timedelta(
            days=31)

        active = Opportunity.objects.create(id="1", name="1",
                                            start=today - timedelta(days=1),
                                            end=today + timedelta(days=1),
                                            probability=100)
        completed = Opportunity.objects.create(id="2", name="2",
                                               start=month_ago, end=month_ago,
                                               probability=100)
        upcoming = Opportunity.objects.create(id="3", name="3",
                                              start=month_after,
                                              end=month_after, probability=100)

        filters = dict(
            period="custom",
            start=month_ago,
            end=month_after,
        )

        response = self.client.get("{}?{}".format(
            self.url,
            urlencode(dict(status="active", **filters))
        ))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]['id'], active.id)

        response = self.client.get(
            "{}?{}".format(self.url,
                           urlencode(dict(status="upcoming", **filters))))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]['id'], upcoming.id)

        response = self.client.get(
            "{}?{}".format(self.url,
                           urlencode(dict(status="completed", **filters))))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]['id'], completed.id)

        response = self.client.get(
            "{}?{}".format(self.url,
                           urlencode(dict(status="any", **filters))))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 3)

    def test_get_opportunities_sort_by(self):
        today = timezone.now()
        first = Opportunity.objects.create(
            id="1", name="A", start=today - timedelta(days=1),
            end=today + timedelta(days=1), probability=100,
        )
        second = Opportunity.objects.create(
            id="2", name="Z", start=today - timedelta(days=1),
            end=today + timedelta(days=1), probability=100,
        )

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items"][0]['id'], first.id)

        response = self.client.get("{}?sort_by=-account".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)
        self.assertEqual(response.data["items"][0]['id'], second.id)
        self.assertEqual(response.data["items"][1]['id'], first.id)

    def test_get_opportunities_dynamic_placement_rate_tech_fee_margin(self):
        today = timezone.now()
        start, end = today - timedelta(days=1), today + timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id="1", name="A", start=start, end=end,
            probability=100)
        tech_fee = 0.12
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            tech_fee=tech_fee,
            goal_type_id=SalesForceGoalType.CPV,
            tech_fee_type=OpPlacement.TECH_FEE_CPV_TYPE,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE)
        Flight.objects.create(placement=placement, total_cost=510, cost=123,
                              start=start, end=end)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign, cost=123,
                                         video_views=3214, date=start)
        stats = CampaignStatistic.objects.filter(campaign=campaign) \
            .aggregate(cost=Sum("cost"), views=Sum("video_views"))
        cpv = stats["cost"] / stats["views"]
        expected_margin = tech_fee / (cpv + tech_fee) * 100
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertAlmostEqual(response.data["items"][0]['margin'],
                               expected_margin)

    def test_video_view_rate_shows_percents(self):
        """
        Bug: https://channelfactory.atlassian.net/browse/IQD-2701
        Summary: Pacing report > View rate parameter isn't multiplied on 100%
        """

        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="A", start=today - timedelta(days=1),
            end=today + timedelta(days=1), probability=100
        )
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
        )
        Flight.objects.create(placement=placement,
                              start=today,
                              end=today)
        campaign = Campaign.objects.create(salesforce_placement=placement,
                                           video_views=1)
        CampaignStatistic.objects.create(date=today,
                                         campaign=campaign,
                                         video_views=35,
                                         impressions=100)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        opp_data = response.data["items"][0]
        self.assertEqual(opp_data["video_view_rate"], 35.)

    def test_opportunity_margin_zero_cost(self):
        expected_margin = 100
        now = now_in_default_tz()
        today = now.date()
        opportunity = Opportunity.objects.create(
            id="1", name="A", start=today - timedelta(days=1),
            end=today + timedelta(days=1), probability=100
        )
        placement = OpPlacement.objects.create(
            goal_type_id=SalesForceGoalType.HARD_COST,
            opportunity=opportunity, total_cost=1)
        Flight.objects.create(placement=placement,
                              start=today, end=today,
                              total_cost=1)
        Campaign.objects.create(
            salesforce_placement=placement, cost=0)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        opp_data = response.data["items"][0]
        self.assertEqual(opp_data["margin"], expected_margin)

    def test_opportunity_margin_zero_total_cost(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="A", start=today - timedelta(days=1),
            end=today + timedelta(days=1), probability=100
        )
        placement = OpPlacement.objects.create(
            goal_type_id=SalesForceGoalType.CPM,
            opportunity=opportunity, total_cost=0)
        Flight.objects.create(placement=placement, start=today, end=today)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today,
                                         cost=1)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        opp_data = response.data["items"][0]
        self.assertEqual(opp_data["margin"], -100)

    def test_opportunity_margin(self):
        now = now_in_default_tz()
        today = now.date()
        placement_hc_cost = 10
        campaign_cost = 20
        placement_cpv_ordered_rate = 50
        campaign_cpv_video_views = 10
        hard_cost_client_cost = 400
        cost = placement_hc_cost + campaign_cost
        cpv_client_cost = placement_cpv_ordered_rate * campaign_cpv_video_views
        client_cost = hard_cost_client_cost + cpv_client_cost
        expected_margin = (1 - cost / client_cost) * 100
        opportunity = Opportunity.objects.create(
            id="1", name="A", start=today - timedelta(days=1),
            end=today + timedelta(days=1), probability=100, budget=600)
        placement_hc = OpPlacement.objects.create(
            id="1", goal_type_id=SalesForceGoalType.HARD_COST,
            opportunity=opportunity)
        placement_cpv = OpPlacement.objects.create(
            id="2", goal_type_id=SalesForceGoalType.CPV,
            opportunity=opportunity, ordered_rate=placement_cpv_ordered_rate)
        Flight.objects.create(id="1", placement=placement_hc,
                              start=today, end=today,
                              total_cost=hard_cost_client_cost,
                              cost=placement_hc_cost)
        Flight.objects.create(id="2", placement=placement_cpv,
                              start=today, end=today, total_cost=1000)
        campaign = Campaign.objects.create(
            id="2", salesforce_placement=placement_cpv,
            cost=campaign_cost)
        CampaignStatistic.objects.create(date=today,
                                         campaign=campaign,
                                         cost=campaign_cost,
                                         video_views=campaign_cpv_video_views)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        opp_data = response.data["items"][0]
        self.assertEqual(opp_data["margin"], expected_margin)

    def test_cpv_dynamic_placement_rate_and_tech_fee_today_goal(self):
        today = date(2017, 12, 8)
        start, end = date(2017, 12, 1), date(2017, 12, 31)
        total_cost, last_3_days_cost = 988.12, 366.87
        total_views, last_3_days_views = 23199, 8520
        client_budget = 5000
        tech_fee = 0.015

        days_remaining = (end - today).days + 1
        not_relevant_cost = total_cost - last_3_days_cost
        not_relevant_views = total_views - last_3_days_views
        last_3_days_cpv = last_3_days_cost / last_3_days_views
        total_cpv = total_cost / total_views
        cost_spent = (total_cpv + tech_fee) * total_views
        budget_remaining = client_budget - cost_spent
        total_pacing_goal = last_3_days_cpv * budget_remaining \
                            / (last_3_days_cpv + tech_fee)
        expected_budget = total_pacing_goal / days_remaining
        opportunity = Opportunity.objects.create(probability=100,
                                                 budget=client_budget,
                                                 start=start, end=end)
        placement = OpPlacement.objects.create(
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=tech_fee)
        Flight.objects.create(placement=placement,
                              start=start, end=end,
                              total_cost=client_budget)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(date=today - timedelta(days=4),
                                         campaign=campaign,
                                         cost=not_relevant_cost,
                                         video_views=not_relevant_views)
        CampaignStatistic.objects.create(date=today - timedelta(days=3),
                                         campaign=campaign,
                                         cost=last_3_days_cost,
                                         video_views=last_3_days_views)
        with patch_now(today):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        chart_data = response.data["items"][0]["chart_data"]["budget"]
        self.assertAlmostEqual(chart_data["today_budget"], expected_budget)

    def test_cpm_dynamic_placement_rate_and_tech_fee_today_goal(self):
        today = date(2017, 12, 8)
        start, end = date(2017, 12, 1), date(2017, 12, 31)
        total_cost, last_3_days_cost = 31967, 366.87
        total_impressions, last_3_days_impressions = 5731696, 85001
        client_budget = 80000
        tech_fee = 0.015

        days_remaining = (end - today).days + 1
        not_relevant_cost = total_cost - last_3_days_cost
        not_relevant_impressions = total_impressions - last_3_days_impressions
        last_3_days_cpm = last_3_days_cost / last_3_days_impressions * 1000
        total_cpm = total_cost * 1. / total_impressions * 1000
        cost_spent = (total_cpm + tech_fee) * total_impressions / 1000
        budget_remaining = client_budget - cost_spent
        total_pacing_goal = last_3_days_cpm * budget_remaining \
                            / (last_3_days_cpm + tech_fee)
        expected_budget = total_pacing_goal / days_remaining
        opportunity = Opportunity.objects.create(probability=100,
                                                 budget=client_budget,
                                                 start=start, end=end)
        placement = OpPlacement.objects.create(
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee=tech_fee)
        Flight.objects.create(placement=placement,
                              start=start, end=end,
                              total_cost=client_budget)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(date=today - timedelta(days=4),
                                         campaign=campaign,
                                         cost=not_relevant_cost,
                                         impressions=not_relevant_impressions)
        CampaignStatistic.objects.create(date=today - timedelta(days=3),
                                         campaign=campaign,
                                         cost=last_3_days_cost,
                                         impressions=last_3_days_impressions)
        with patch_now(today):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        chart_data = response.data["items"][0]["chart_data"]["budget"]
        self.assertAlmostEqual(chart_data["today_budget"], expected_budget)

    def test_dynamic_placement_negative(self):
        today = date(2017, 1, 1)
        opportunity = Opportunity.objects.create(probability=100,
                                                 start=today, end=today)

        OpPlacement.objects.create(
            id=1,
            opportunity=opportunity,
            start=today - timedelta(days=1),
            end=today + timedelta(days=1)
        )

        with patch_now(today):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertFalse(response.data["items"][0]["has_dynamic_placements"])

    def test_dynamic_placement_positive(self):
        today = date(2017, 1, 1)
        dates = [
            (today - timedelta(days=2), today - timedelta(days=1)),
            (today - timedelta(days=1), today + timedelta(days=1)),
            (today + timedelta(days=1), today + timedelta(days=2)),
        ]
        test_data = tuple(product(dates, DYNAMIC_PLACEMENT_TYPES))
        count = len(test_data)
        for index, item in enumerate(test_data):
            start_end, dynamic_placement = item
            start, end = start_end
            opportunity = Opportunity.objects.create(id=index, probability=100,
                                                     start=today, end=today)
            OpPlacement.objects.create(
                id=index,
                opportunity=opportunity,
                dynamic_placement=dynamic_placement,
                start=start,
                end=end
            )
        with patch_now(today):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), count)
        self.assertTrue(all(o["has_dynamic_placements"]
                            for o in response.data["items"]))

    def test_dynamic_placement_budget(self):
        today = date(2017, 1, 1)
        tz = "UTC"
        last_update = datetime.combine(today, time.min).replace(tzinfo=pytz.timezone(tz))
        start = today - timedelta(days=3)
        end = today + timedelta(days=3)
        total_days = (end - start).days + 1
        days_pass = (today - start).days
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end, probability=100
        )
        total_cost = 123
        aw_cost = 23
        views, impressions = 14, 164
        aw_cpv = aw_cost * 1. / views
        aw_cpm = aw_cost * 1000. / impressions
        expected_pacing = aw_cost / (total_cost / total_days * days_pass) * 100
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start, end=end, total_cost=total_cost,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.BUDGET,
        )
        Flight.objects.create(placement=placement, start=start, end=end,
                              total_cost=total_cost)
        account = Account.objects.create(timezone=tz, update_time=last_update)
        campaign = Campaign.objects.create(account=account,
                                           salesforce_placement=placement,
                                           video_views=1)
        CampaignStatistic.objects.create(date=today, campaign=campaign,
                                         cost=aw_cost,
                                         video_views=views,
                                         impressions=impressions)
        with patch_now(today):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        pl = response.data["items"][0]
        self.assertIsNone(pl["plan_video_views"])
        self.assertIsNone(pl["plan_impressions"])
        self.assertIsNone(pl["plan_cpv"])
        self.assertIsNone(pl["plan_cpm"])
        self.assertEqual(pl["plan_cost"], total_cost)
        self.assertEqual(pl["cost"], aw_cost)
        self.assertEqual(pl["cpv"], aw_cpv)
        self.assertEqual(pl["cpm"], aw_cpm)
        self.assertEqual(pl["impressions"], impressions)
        self.assertEqual(pl["video_views"], views)
        self.assertAlmostEqual(pl["pacing"], expected_pacing)
        self.assertAlmostEqual(pl["margin"], 0)

    def test_dynamic_placement_budget_over_delivery_margin(self):
        today = date(2017, 1, 1)
        start = today - timedelta(days=3)
        end = today + timedelta(days=3)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=start, end=end, probability=100
        )
        total_cost = 123
        aw_cost = 234
        assert aw_cost > total_cost
        views, impressions = 14, 164
        expected_margin = (total_cost - aw_cost) / total_cost * 100.
        placement = OpPlacement.objects.create(
            id="1", name="BBB", opportunity=opportunity,
            start=start, end=end, total_cost=total_cost,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.BUDGET,
        )
        Flight.objects.create(placement=placement, start=start, end=end,
                              total_cost=total_cost)
        campaign = Campaign.objects.create(salesforce_placement=placement,
                                           video_views=1)
        CampaignStatistic.objects.create(date=today, campaign=campaign,
                                         cost=aw_cost,
                                         video_views=views,
                                         impressions=impressions)
        with patch_now(today):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        pl = response.data["items"][0]
        self.assertAlmostEqual(pl["margin"], expected_margin)

    def test_no_dates(self):
        Opportunity.objects.create(
            id="1", name="1", start=None, end=None, probability=100
        )
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["status"], "undefined")

    def test_pagination(self):
        response = self.client.get(self.url + "?page=1")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, {
            "items": [],
            "current_page": 1,
            "items_count": 0,
            "max_page": 1
        })

    def test_opportunity_chart_data(self):
        """
        Daily Pacing
        {'label': datetime.date(2017, 1, 1), 'value': 102.0}
        {'label': datetime.date(2017, 1, 2), 'value': 204.0}
        {'label': datetime.date(2017, 1, 3), 'value': 306.0}
        {'label': datetime.date(2017, 1, 4), 'value': 408.0}
        {'label': datetime.date(2017, 1, 5), 'value': 408.0}
        {'label': datetime.date(2017, 1, 6), 'value': 408.0}
        {'label': datetime.date(2017, 1, 7), 'value': 408.0+51.0=459.0}
        {'label': datetime.date(2017, 1, 8), 'value': 408.0+102.0=510.0}

        Delivery Chart
        {'label': datetime.date(2017, 1, 1), 'value': 102}
        {'label': datetime.date(2017, 1, 2), 'value': 204}
        {'label': datetime.date(2017, 1, 3), 'value': 306}

        2017-01-04
        Today goal: 102
        Today budget: 102 * 0.5
        :return:
        """
        start, end = date(2017, 1, 1), date(2017, 1, 8)
        today = datetime(2017, 1, 4).date()
        opportunity = Opportunity.objects.create(
            id="1", name="1", probability=100, budget=100, start=start, end=end,
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        first_end = start + timedelta(days=3)
        first_flight = Flight.objects.create(
            id="1", name="", placement=placement,
            start=start, end=first_end, ordered_units=400,
        )

        second_start = first_end + timedelta(days=3)
        second_flight = Flight.objects.create(
            id="2", name="", placement=placement,
            start=second_start, end=end, ordered_units=100,
        )

        campaign = Campaign.objects.create(id="1", name="",
                                           salesforce_placement=placement)
        for i in range(3):
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=start + timedelta(days=i),
                                             video_views=102, cost=51)

        with patch_now(today):
            response = self.client.get(self.url)
        opportunities = response.data["items"]
        self.assertEqual(len(opportunities), 1)

        opportunity_data = opportunities[0]["chart_data"]["cpv"]
        self.assertEqual(opportunity_data["today_goal"], 102)
        self.assertEqual(opportunity_data["today_budget"], 51)
        self.assertEqual(opportunity_data["yesterday_delivered"], 102)
        self.assertEqual(opportunity_data["yesterday_budget"], 51)

        charts = opportunity_data["charts"]
        self.assertEqual(len(charts), 4)

        pacing_chart = charts[0]
        self.assertEqual(pacing_chart["title"], "Ideal Pacing")
        self.assertEqual(len(pacing_chart["data"]),
                         (second_flight.end - first_flight.start).days + 1)
        self.assertEqual(pacing_chart["data"][3]["value"],
                         first_flight.ordered_units * 1.02)
        self.assertEqual(pacing_chart["data"][5]["value"],
                         first_flight.ordered_units * 1.02)
        self.assertEqual(pacing_chart["data"][-1]["value"],
                         first_flight.ordered_units * 1.02 + second_flight.ordered_units * 1.02)

        delivery_chart = charts[1]
        self.assertEqual(delivery_chart["title"], "Daily Deviation")
        self.assertEqual(len(delivery_chart["data"]), 3)
        self.assertEqual(delivery_chart["data"][-1]["value"], 306)

    def test_opportunity_cpv_and_cpm_chart_data(self):
        """
        We expect different chart blocks for every placement type (like CPV or CPM)
        :return:
        """
        start, end = datetime(2017, 1, 1).date(), datetime(2017, 1, 5).date()
        today = datetime(2017, 1, 4).date()
        opportunity = Opportunity.objects.create(
            id="1", name="1", probability=100, budget=100, start=start, end=end,
        )
        # CPV
        placement_cpv = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, start=start, end=end,
        )
        flight_cpv = Flight.objects.create(
            id="1", name="", placement=placement_cpv, start=start, end=end,
            ordered_units=500,
        )
        campaign = Campaign.objects.create(id="1", name="",
                                           salesforce_placement=placement_cpv)
        for i in range(3):
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=start + timedelta(days=i),
                                             video_views=102, cost=51)
        # CPM
        placement_cpm = OpPlacement.objects.create(
            id="2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM, start=start, end=end,
        )
        flight_cpm = Flight.objects.create(
            id="2", name="", placement=placement_cpm, start=start, end=end,
            ordered_units=5000,
        )
        campaign = Campaign.objects.create(id="2", name="",
                                           salesforce_placement=placement_cpm)
        for i in range(3):
            CampaignStatistic.objects.create(campaign=campaign,
                                             date=start + timedelta(days=i),
                                             impressions=1020, cost=3)

        with patch_now(today):
            response = self.client.get(self.url)
        opportunities = response.data["items"]
        self.assertEqual(len(opportunities), 1)

        opportunity_data = opportunities[0]

        # CPV tests
        cpv_chart_data = opportunity_data["chart_data"]["cpv"]
        self.assertEqual(cpv_chart_data["today_goal"], 102)
        self.assertEqual(cpv_chart_data["today_budget"], 51)
        self.assertEqual(cpv_chart_data["yesterday_delivered"], 102)
        self.assertEqual(cpv_chart_data["yesterday_budget"], 51)
        self.assertEqual(cpv_chart_data["targeting"]["video_views"], 306)

        charts = cpv_chart_data["charts"]
        self.assertEqual(len(charts), 4)

        pacing_chart = charts[0]
        self.assertEqual(pacing_chart["title"], "Ideal Pacing")
        self.assertEqual(len(pacing_chart["data"]), (end - start).days + 1)
        self.assertEqual(pacing_chart["data"][-1]["value"],
                         flight_cpv.ordered_units * 1.02)

        delivery_chart = charts[1]
        self.assertEqual(delivery_chart["title"], "Daily Deviation")
        self.assertEqual(len(delivery_chart["data"]), 3)
        self.assertEqual(delivery_chart["data"][-1]["value"], 306)

        # CPM tests
        cpm_chart_data = opportunity_data["chart_data"]["cpm"]
        self.assertEqual(cpm_chart_data["today_goal"], 1020)
        self.assertAlmostEqual(cpm_chart_data["today_budget"], 3, places=10)
        self.assertEqual(cpm_chart_data["yesterday_delivered"], 1020)
        self.assertEqual(cpm_chart_data["yesterday_budget"], 3)
        self.assertEqual(cpm_chart_data["targeting"]["impressions"], 3060)

        charts = cpm_chart_data["charts"]
        self.assertEqual(len(charts), 4)

        pacing_chart = charts[0]
        self.assertEqual(pacing_chart["title"], "Ideal Pacing")
        self.assertEqual(len(pacing_chart["data"]), (end - start).days + 1)
        self.assertEqual(pacing_chart["data"][-1]["value"],
                         flight_cpm.ordered_units * 1.02)

        delivery_chart = charts[1]
        self.assertEqual(delivery_chart["title"], "Daily Deviation")
        self.assertEqual(len(delivery_chart["data"]), 3)
        self.assertEqual(delivery_chart["data"][-1]["value"], 3060)

    def test_get_opportunities_filter_category_with_coma(self):
        category1 = Category.objects.create(id="category, value 1")
        category2 = Category.objects.create(id="category, value 2")
        category3 = Category.objects.create(id="category, value 3")
        today = timezone.now()
        opp_1 = Opportunity.objects.create(id="1", name="1", start=today,
                                           end=today,
                                           category=category1, probability=100)
        opp_2 = Opportunity.objects.create(id="2", name="2", start=today,
                                           end=today,
                                           category=category2, probability=100)
        Opportunity.objects.create(id="3", name="3", start=today,
                                   end=today, category=category3,
                                   probability=100)

        query_params = QueryDict("", mutable=True)
        query_params.update(category=category1.id)
        query_params.update(category=category2.id)

        query_url = "?".join([self.url, query_params.urlencode()])
        response = self.client.get(query_url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(response.data["items_count"], 2)
        response_ids = [i["id"] for i in data]
        self.assertEqual(set(response_ids), {opp_1.id, opp_2.id})

    def test_opportunity_chart_rate_and_tech_fee(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/SAAS-2481
        :return:
        """
        today = date(2018, 5, 11)
        start, end = date(2018, 5, 1), date(2018, 6, 30)
        start_1, end_1 = date(2018, 5, 1), date(2018, 5, 31)
        start_2, end_2 = date(2018, 6, 1), date(2018, 6, 30)
        total_costs = (16969.7, 16422.3)
        opportunity = Opportunity.objects.create(probability=100)
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            start=start, end=end,
            goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            total_cost=sum(total_costs),
            ordered_rate=0.045,
            tech_fee=0.01)
        flight_1 = Flight.objects.create(id=1, placement=placement,
                                         start=start_1, end=end_1,
                                         total_cost=total_costs[0])
        flight_2 = Flight.objects.create(id=2, placement=placement,
                                         start=start_2, end=end_2,
                                         total_cost=total_costs[1])
        campaign = Campaign.objects.create(salesforce_placement=placement)

        delivery = (
            (date(2018, 5, 7), 118381, 1663.79),  # before last 3 days
            (date(2018, 5, 8), 7844, 121.53),
            (date(2018, 5, 9), 17513, 277.7),
            (date(2018, 5, 10), 17689, 278.98),  # yesterday
        )
        spent_cost = sum(i[2] for i in delivery)
        cost_left = flight_1.total_cost - spent_cost
        days_left_1 = (end_1 - today).days + 1
        days_left_2 = (end_2 - start_2).days + 1
        daily_plan_1 = cost_left / days_left_1
        daily_plan_2 = flight_2.total_cost / days_left_2
        for dt, views, cost in delivery:
            CampaignStatistic.objects.create(campaign=campaign, date=dt,
                                             video_views=views, cost=cost)

        with patch_now(today):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        opp_data = response.data["items"][0]
        self.assertIn("budget", opp_data["chart_data"])
        self.assertNotIn("cpm", opp_data["chart_data"])
        self.assertNotIn("cpv", opp_data["chart_data"])
        cpv_charts = opp_data["chart_data"]["budget"]["charts"]
        cpv_charts_map = dict((c["id"], c["data"]) for c in cpv_charts)
        self.assertIn(PacingReportChartId.IDEAL_PACING, cpv_charts_map)
        ideal_pacing = cpv_charts_map.get(PacingReportChartId.IDEAL_PACING)
        ideal_pacing_by_date = dict((item["label"], item["value"])
                                    for item in ideal_pacing)

        for i in range(days_left_1):
            dt = today + timedelta(days=i)
            self.assertAlmostEqual(
                ideal_pacing_by_date[dt],
                spent_cost + daily_plan_1 * (i + 1),
                msg=dt)

        for i in range(days_left_2):
            dt = start_2 + timedelta(days=i)
            self.assertAlmostEqual(
                ideal_pacing_by_date[dt],
                flight_1.total_cost + daily_plan_2 * (i + 1),
                msg=dt)

        self.assertAlmostEqual(ideal_pacing[-1]["value"], placement.total_cost)

    def test_success_on_cpm_and_cpv(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/SAAS-2514
        Summary: Pacing report > 500 error occurs
                 when a user filters by CPV&CPM goal type
        """
        today = date(2017, 1, 1)
        opportunity = Opportunity.objects.create(
            probability=100, goal_type_id=SalesForceGoalType.CPM_AND_CPV)
        common_data = dict(start=today, end=today, total_cost=123)
        placement_data = dict(opportunity=opportunity,
                              dynamic_placement=DynamicPlacementType.BUDGET,
                              **common_data)
        placement_1 = OpPlacement.objects.create(
            id=1, goal_type_id=SalesForceGoalType.CPM, **placement_data)
        placement_2 = OpPlacement.objects.create(
            id=2, goal_type_id=SalesForceGoalType.CPV, **placement_data)
        Flight.objects.create(id=1, placement=placement_1, **common_data)
        Flight.objects.create(id=2, placement=placement_2, **common_data)

        filters = dict(
            goal_type=SalesForceGoalType.CPM_AND_CPV
        )
        with patch_now(today):
            response = self.client.get("{}?{}".format(
                self.url,
                urlencode(filters)
            ))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(set(response.data["items"][0]["chart_data"].keys()),
                         {"budget"})

    def test_custom_date_range_filter(self):
        search_date = date(2018, 5, 22)
        Opportunity.objects.create(id=1, probability=100)
        Opportunity.objects.create(id=2, probability=100,
                                   start=search_date - timedelta(days=1),
                                   end=search_date - timedelta(days=1))
        Opportunity.objects.create(id=3, probability=100,
                                   start=search_date + timedelta(days=1),
                                   end=search_date + timedelta(days=1))
        opportunity = Opportunity.objects.create(
            id=4, probability=100,
            start=search_date - timedelta(days=1),
            end=search_date + timedelta(days=1))
        opportunity.refresh_from_db()

        filters = dict(
            start=str(search_date),
            end=str(search_date),
        )
        response = self.client.get("{}?{}".format(
            self.url,
            urlencode(filters)
        ))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_ctr_multiply_by_100(self):
        any_date = date(2018, 1, 1)
        clicks, views, impressions = 234, 542, 654
        expected_ctr = clicks / impressions * 100
        expected_ctr_v = clicks / views * 100

        def create_opportunity(uid, goal_type_id, create_statistic=True):
            opportunity = Opportunity.objects.create(id=uid, probability=100)
            placement = OpPlacement.objects.create(id=uid,
                                                   opportunity=opportunity,
                                                   goal_type_id=goal_type_id)
            Flight.objects.create(id=uid, placement=placement, start=any_date,
                                  end=any_date)
            campaign = Campaign.objects.create(id=uid,
                                               salesforce_placement=placement,
                                               video_views=1)
            if create_statistic:
                CampaignStatistic.objects.create(date=any_date,
                                                 campaign=campaign,
                                                 clicks=clicks,
                                                 video_views=views,
                                                 impressions=impressions)
            opportunity.refresh_from_db()
            return opportunity

        cpv_opportunity = create_opportunity(1, SalesForceGoalType.CPV)
        cpm_opportunity = create_opportunity(2, SalesForceGoalType.CPM)
        cpv_opportunity_no_statistic = create_opportunity(
            3, SalesForceGoalType.CPV, False)
        cpm_opportunity_no_statistic = create_opportunity(
            4, SalesForceGoalType.CPM, False)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 4)
        ctr_by_id = {o["id"]: o["ctr"] for o in response.data["items"]}
        self.assertAlmostEqual(ctr_by_id[cpv_opportunity.id], expected_ctr_v)
        self.assertAlmostEqual(ctr_by_id[cpm_opportunity.id], expected_ctr)
        self.assertIsNone(ctr_by_id[cpv_opportunity_no_statistic.id])
        self.assertIsNone(ctr_by_id[cpm_opportunity_no_statistic.id])

    def test_dynamic_placements_types_does_not_contain_nones(self):
        opportunity = Opportunity.objects.create(probability=100)
        OpPlacement.objects.create(id=1, opportunity=opportunity,
                                   dynamic_placement=None)
        OpPlacement.objects.create(
            id=2, opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET)
        OpPlacement.objects.create(
            id=3, opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertTrue(response.data["items"][0]["has_dynamic_placements"])
        self.assertEqual(response.data["items"][0]["dynamic_placements_types"],
                         [DynamicPlacementType.BUDGET])

    def test_not_started_placement_should_not_affect_margin(self):
        today = date(2018, 1, 1)
        tomorrow = today + timedelta(days=1)
        opportunity = Opportunity.objects.create(probability=100)
        total_costs = (500, 600)
        aw_cost = sum(total_costs)
        placement = OpPlacement.objects.create(
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            dynamic_placement=DynamicPlacementType.BUDGET,
            total_cost=sum(total_costs))
        flight_include = Flight.objects.create(id=1, placement=placement,
                                               start=today, end=today,
                                               total_cost=total_costs[0])
        flight_exclude = Flight.objects.create(id=2, placement=placement,
                                               start=tomorrow, end=tomorrow,
                                               total_cost=total_costs[1])
        self.assertGreater(flight_exclude.start, today)
        self.assertGreater(aw_cost, flight_include.total_cost)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(date=today,
                                         campaign=campaign,
                                         cost=aw_cost)
        expected_margin = (1 - aw_cost / flight_include.total_cost) * 100

        with patch_now(today):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertAlmostEqual(response.data["items"][0]["margin"],
                               expected_margin)

    def test_billing_server(self):
        opportunity_1 = Opportunity.objects.create(
            id=1, billing_server="test 1", probability=100)
        opportunity_2 = Opportunity.objects.create(
            id=2, billing_server="test 2", probability=100)
        opportunity_1.refresh_from_db()
        opportunity_2.refresh_from_db()

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)
        opps_by_id = {o["id"]: o for o in response.data["items"]}
        opp_1 = opps_by_id[opportunity_1.id]
        opp_2 = opps_by_id[opportunity_2.id]
        self.assertEqual(opp_1["billing_server"], opportunity_1.billing_server)
        self.assertEqual(opp_2["billing_server"], opportunity_2.billing_server)

    def test_hard_cost_margin(self):
        today = date(2018, 1, 1)
        total_cost = 6543
        our_cost = 1234
        days_pass, days_left = 3, 6
        total_days = days_pass + days_left
        self.assertGreater(days_pass, 0)
        self.assertGreater(days_left, 0)
        start = today - timedelta(days=(days_pass - 1))
        end = today + timedelta(days=days_left)
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3), probability=100)
        hard_cost_placement = OpPlacement.objects.create(
            id="2", name="Hard cost placement", opportunity=opportunity,
            start=start, end=end,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(
            start=start, end=end, total_cost=total_cost,
            placement=hard_cost_placement, cost=our_cost)
        client_cost = total_cost / total_days * days_pass
        expected_margin = (1 - our_cost / client_cost) * 100
        with patch_now(today):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["margin"], expected_margin)

    def test_outgoing_fee_pacing(self):
        today = date(2018, 1, 1)
        tz = "UTC"
        last_update = datetime.combine(today, time.min).replace(tzinfo=pytz.timezone(tz))
        total_cost = 6543
        our_cost = 1234
        days_pass, days_left = 3, 6
        total_days = days_pass + days_left
        self.assertGreater(days_pass, 0)
        self.assertGreater(days_left, 0)
        start = today - timedelta(days=days_pass)
        end = today + timedelta(days=days_left - 1)
        opportunity = Opportunity.objects.create(
            id=next(int_iterator), name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3), probability=100)
        placement_outgoing_fee = OpPlacement.objects.create(
            id=next(int_iterator), name="Outgoing fee placement", opportunity=opportunity,
            start=start, end=end, placement_type=OpPlacement.OUTGOING_FEE_TYPE,
            goal_type_id=SalesForceGoalType.CPV)
        placement_cpv = OpPlacement.objects.create(
            id=next(int_iterator), name="Outgoing fee placement",
            opportunity=opportunity,
            start=start, end=end,
            goal_type_id=SalesForceGoalType.CPV)
        Flight.objects.create(
            id=next(int_iterator),
            start=start, end=end, total_cost=0,
            ordered_units=2100,
            delivered=2100,
            placement=placement_outgoing_fee, cost=87)
        plan_units, units_delivered = 123, 12
        units_by_yesterday = plan_units * PacingReport.goal_factor * days_pass / total_days

        Flight.objects.create(
            id=next(int_iterator),
            start=start, end=end, total_cost=total_cost, ordered_units=plan_units,
            placement=placement_cpv, cost=1)
        account = Account.objects.create(timezone=tz, update_time=last_update)
        campaign_outgoing_fee = Campaign.objects.create(
            id=next(int_iterator),
            account=account,
            salesforce_placement=placement_outgoing_fee
        )
        campaign_cpv = Campaign.objects.create(
            id=next(int_iterator),
            account=account,
            salesforce_placement=placement_cpv
        )
        CampaignStatistic.objects.create(date=start, campaign=campaign_outgoing_fee,
                                         cost=our_cost)
        CampaignStatistic.objects.create(
            date=start, campaign=campaign_cpv,
            video_views=units_delivered, cost=our_cost)
        expected_pacing = (units_delivered / units_by_yesterday) * 100
        with patch_now(today):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertAlmostEqual(response.data["items"][0]["pacing"], expected_pacing)

    def test_outgoing_fee_margin(self):
        today = date(2018, 1, 1)
        tz = "UTC"
        last_update = datetime.combine(today, time.min).replace(tzinfo=pytz.timezone(tz))
        our_cost = 1234
        days_pass, days_left = 3, 6
        self.assertGreater(days_pass, 0)
        self.assertGreater(days_left, 0)
        start = today - timedelta(days=days_pass)
        end = today + timedelta(days=days_left - 1)
        opportunity = Opportunity.objects.create(
            id=next(int_iterator), name="1", start=today - timedelta(days=3),
            end=today + timedelta(days=3), probability=100)
        placement_outgoing_fee = OpPlacement.objects.create(
            id=next(int_iterator), name="Outgoing fee placement", opportunity=opportunity,
            start=start, end=end, placement_type=OpPlacement.OUTGOING_FEE_TYPE,
            goal_type_id=SalesForceGoalType.CPV, ordered_units=123)
        Flight.objects.create(
            id=next(int_iterator),
            start=start, end=end, total_cost=0,
            ordered_units=2100,
            delivered=2100,
            placement=placement_outgoing_fee, cost=87)

        account = Account.objects.create(timezone=tz, update_time=last_update)
        campaign_outgoing_fee = Campaign.objects.create(
            account=account,
            salesforce_placement=placement_outgoing_fee
        )
        CampaignStatistic.objects.create(date=start, campaign=campaign_outgoing_fee,
                                         cost=our_cost, video_views=13)
        with patch_now(today):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["margin"], -100)

    @generic_test([
        (global_account_visibility, (global_account_visibility, count), dict())
        for global_account_visibility, count in ((True, 0), (False, 1))
    ])
    def test_global_account_visibility(self, global_account_visibility, expected_count):
        Opportunity.objects.create(id=next(int_iterator), probability=100)
        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: global_account_visibility,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: []
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], expected_count)

    def test_shows_last_account_update_time(self):
        test_update_time = datetime(2018, 10, 11, 12, 13, 14, tzinfo=pytz.utc)
        any_date = date(2018, 1, 1)
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
        )
        Flight.objects.create(placement=placement, start=any_date, end=any_date)
        account = Account.objects.create(update_time=test_update_time)
        Campaign.objects.create(account=account, salesforce_placement=placement)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        opportunity_data = response.data["items"][0]
        self.assertEqual(opportunity_data["aw_update_time"], test_update_time.strftime("%Y-%m-%dT%H:%M:%SZ"))

    def test_goal_on_recalculation(self):
        now = datetime(2018, 10, 10, 10, 10)
        today = now.date()
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        placement = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        ordered_units = 100
        total_ordered_units = 0
        for dt in [yesterday, today, tomorrow]:
            flight = Flight.objects.create(
                id=next(int_iterator),
                placement=placement,
                ordered_units=ordered_units,
                start=dt,
                end=dt,
            )
            total_ordered_units += flight.ordered_units

        campaign = Campaign.objects.create(
            id=next(int_iterator),
            salesforce_placement=placement
        )
        CampaignStatistic.objects.create(
            date=yesterday,
            campaign=campaign,
            video_views=ordered_units * 2,
        )

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings), \
             patch_now(now):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        expected_plan_views = total_ordered_units * PacingReport.goal_factor
        self.assertAlmostEqual(response.data["items"][0]["plan_video_views"], expected_plan_views)

    def test_delivery_plan(self):
        now = datetime(2018, 10, 10, 10, 10)
        today = now.date()
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        placement = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        ordered_units = 100
        total_ordered_units = 0
        for dt in [yesterday, today, tomorrow]:
            flight = Flight.objects.create(
                id=next(int_iterator),
                placement=placement,
                ordered_units=ordered_units,
                start=dt,
                end=dt,
            )
            total_ordered_units += flight.ordered_units

        campaign = Campaign.objects.create(
            id=next(int_iterator),
            salesforce_placement=placement
        )
        CampaignStatistic.objects.create(
            date=yesterday,
            campaign=campaign,
            video_views=ordered_units * 2,
        )

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings), \
             patch_now(now):
            response = self.client.get(self.url)

        expected_plan_views = total_ordered_units * PacingReport.goal_factor
        daily_plan = expected_plan_views / 3
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        charts = response.data["items"][0]["chart_data"]["cpv"]["charts"]
        data_by_id = {i["id"]: i["data"] for i in charts}
        planned_chart = data_by_id.get(PacingReportChartId.PLANNED_DELIVERY)
        self.assertAlmostEqual(planned_chart[0]["value"], daily_plan)
        self.assertAlmostEqual(planned_chart[1]["value"], daily_plan * 2)
        self.assertAlmostEqual(planned_chart[2]["value"], daily_plan * 3)

    def test_historical_goal(self):
        now = datetime(2018, 10, 10, 10, 10)
        today = now.date()
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        placement = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        ordered_units = 100
        flight_finished = Flight.objects.create(
            id=next(int_iterator),
            placement=placement,
            ordered_units=ordered_units,
            start=yesterday,
            end=yesterday,
        )
        flight_started = Flight.objects.create(
            id=next(int_iterator),
            placement=placement,
            ordered_units=ordered_units,
            start=today,
            end=today,
        )
        flight_not_started = Flight.objects.create(
            id=next(int_iterator),
            placement=placement,
            ordered_units=ordered_units,
            start=tomorrow,
            end=tomorrow,
        )

        campaign = Campaign.objects.create(
            id=next(int_iterator),
            salesforce_placement=placement
        )
        delivered = ordered_units * 2.5
        CampaignStatistic.objects.create(
            date=yesterday,
            campaign=campaign,
            video_views=delivered,
        )

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings), \
             patch_now(now):
            response = self.client.get(self.url)

        total_plan_units = sum(flight.ordered_units for flight in Flight.objects.all()) * PacingReport.goal_factor
        current_max = (flight_started.ordered_units + flight_finished.ordered_units) * PacingReport.goal_factor
        over_delivery = delivered - current_max
        consume = min(flight_not_started.ordered_units, over_delivery)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        charts = response.data["items"][0]["chart_data"]["cpv"]["charts"]
        data_by_id = {i["id"]: i["data"] for i in charts}
        historical_goal_chart = data_by_id.get(PacingReportChartId.HISTORICAL_GOAL)
        self.assertEqual(len(historical_goal_chart), 2)
        self.assertAlmostEqual(historical_goal_chart[0]["value"], total_plan_units)
        self.assertAlmostEqual(historical_goal_chart[1]["value"], total_plan_units - consume)

    def test_chart_goal(self):
        now = datetime(2018, 10, 10, 10, 10)
        today = now.date()
        ordered_views = 100
        ordered_impressions = 230
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        cpv_placement = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
        )
        Flight.objects.create(
            id=next(int_iterator),
            placement=cpv_placement,
            ordered_units=ordered_views,
            start=today,
            end=today,
        )
        cpm_placement = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
        )
        Flight.objects.create(
            id=next(int_iterator),
            placement=cpm_placement,
            ordered_units=ordered_impressions,
            start=today,
            end=today,
        )

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings), \
             patch_now(now):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        chart_data = response.data["items"][0]["chart_data"]
        self.assertEqual(chart_data["cpv"]["goal"], ordered_views)
        self.assertEqual(chart_data["cpm"]["goal"], ordered_impressions)

    def test_dynamic_placement_goal(self):
        now = datetime(2018, 10, 10, 10, 10)
        today = now.date()
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        placement = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            dynamic_placement=DynamicPlacementType.BUDGET,
        )
        flight = Flight.objects.create(
            id=next(int_iterator),
            placement=placement,
            ordered_units=1000,
            total_cost=120,
            start=today,
            end=today,
        )

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings), \
             patch_now(now):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        chart_data = response.data["items"][0]["chart_data"]["budget"]
        self.assertEqual(chart_data["goal"], flight.total_cost)

    @generic_test([
        ("True", (True,), dict()),
        ("False", (False,), dict()),
    ])
    def test_margin_cap_required(self, margin_cap_required):
        Opportunity.objects.create(
            id=next(int_iterator),
            margin_cap_required=margin_cap_required,
            probability=100,
        )
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["margin_cap_required"], margin_cap_required)

    def test_outgoing_fee(self):
        opportunity = Opportunity.objects.create(
            id=next(int_iterator),
            probability=100,
        )
        placement = OpPlacement.objects.create(
            id=next(int_iterator),
            opportunity=opportunity,
            placement_type=OpPlacement.OUTGOING_FEE_TYPE,
            goal_type_id=SalesForceGoalType.HARD_COST,
        )
        start = date(2019, 1, 1)
        left, total = 3, 10
        now = start + timedelta(days=left)
        end = start + timedelta(days=total - 1)
        flight = Flight.objects.create(
            id=next(int_iterator),
            placement=placement,
            cost=123,
            start=start,
            end=end,
        )
        expected_spent = flight.cost / total * left

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings), \
             patch_now(now):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertAlmostEqual(response.data["items"][0]["cost"], expected_spent)

    def test_hard_cost_placement_margin_zero_total_cost(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=3),probability=100,
            end=today + timedelta(days=3))
        hard_cost_placement = OpPlacement.objects.create(
            id="2", name="Hard cost placement", opportunity=opportunity,
            start=today - timedelta(days=2), end=today + timedelta(days=2),
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(
            start=today, end=today,
            placement=hard_cost_placement, total_cost=0, cost=1)
        Campaign.objects.create(
            salesforce_placement=hard_cost_placement)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items"][0]["margin"], -100)