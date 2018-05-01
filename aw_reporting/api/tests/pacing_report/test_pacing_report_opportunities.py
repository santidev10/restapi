import logging
from datetime import timedelta, date
from itertools import product
from urllib.parse import urlencode

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from django.db.models import Sum
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_401_UNAUTHORIZED

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Campaign, CampaignStatistic, Flight, \
    Opportunity, Category, User, SalesForceRegions, OpPlacement, \
    SalesForceGoalType
from aw_reporting.models.salesforce_constants import \
    DYNAMIC_PLACEMENT_TYPES, DynamicPlacementType
from saas.urls.namespaces import Namespace
from utils.utils_tests import ExtendedAPITestCase as APITestCase, patch_now

logger = logging.getLogger(__name__)


class PacingReportOpportunitiesTestCase(APITestCase):
    url = reverse(
        Namespace.AW_REPORTING + ":" + Name.PacingReport.OPPORTUNITIES)

    @classmethod
    def setUpClass(cls):
        # The test runner sets DEBUG to False. Set to True to enable SQL logging.
        settings.DEBUG = True
        super(PacingReportOpportunitiesTestCase, cls).setUpClass()

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
        region_id = 1
        current_op = Opportunity.objects.create(
            id="1", name="1", start=today - timedelta(days=1),
            end=today + timedelta(days=1),
            ad_ops_manager=ad_ops_manager,
            category=category, notes=notes,
            region_id=region_id,
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

        data = response.data
        self.assertEqual(len(data), 1)
        item = data[0]
        self.assertEqual(
            set(item.keys()),
            {
                "id", "name", "start", "end", "thumbnail", "cannot_roll_over",
                "status", "is_upcoming", "is_completed",

                "pacing", "pacing_quality", "pacing_direction",
                "margin", "margin_quality", "margin_direction",
                "video_view_rate_quality", "ctr_quality",

                "plan_video_views", "plan_impressions", "plan_cpm", "plan_cpv",
                "goal_type", "plan_cost", "cost", "cpv", "cpm", "impressions",
                "video_views", "video_view_rate", "ctr", "chart_data",

                "ad_ops", "am", "sales", "category", "region", "notes",
                "has_dynamic_placements", "dynamic_placements_types",
                "apex_deal", "bill_of_third_party_numbers",

                "goal_type_ids"
            }
        )
        self.assertEqual(item["id"], current_op.id)
        self.assertEqual(item['status'], "active")
        self.assertEqual(item['thumbnail'], ad_ops_user.profile_image_url)
        self.assertEqual(item['notes'], notes)
        self.assertEqual(item['category']['id'], category.id)
        self.assertEqual(item['category']['name'], category.name)
        self.assertEqual(item['region']['id'], region_id)
        self.assertEqual(item['region']['name'], SalesForceRegions[region_id])

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

        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(response.data[0]['id'], op_expected.id)

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

        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(response.data[0]['id'], op_expected.id)

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

        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(response.data[0]['id'], op_expected.id)

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

        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(response.data[0]['id'], op_expected.id)

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

        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(response.data[0]['id'], op_expected.id)

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

        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(response.data[0]['id'], op_expected.id)

    def test_get_opportunities_filter_region(self):
        today = timezone.now()
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   region_id=0, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="2", start=today,
                                                 end=today, region_id=1,
                                                 probability=100)

        response = self.client.get("{}?region={}".format(self.url, 1))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(response.data[0]['id'], op_expected.id)

    def test_get_opportunities_filter_name(self):
        today = timezone.now()
        Opportunity.objects.create(id="1", name="Peter Griffin", start=today,
                                   end=today, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="Homer Simpson",
                                                 start=today, end=today,
                                                 probability=100)

        response = self.client.get("{}?search={}".format(self.url, "SIM"))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(response.data[0]['id'], op_expected.id)

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
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['id'], active.id)

        response = self.client.get(
            "{}?{}".format(self.url,
                           urlencode(dict(status="upcoming", **filters))))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['id'], upcoming.id)

        response = self.client.get(
            "{}?{}".format(self.url,
                           urlencode(dict(status="completed", **filters))))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['id'], completed.id)

        response = self.client.get(
            "{}?{}".format(self.url,
                           urlencode(dict(status="any", **filters))))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

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
        self.assertEqual(response.data[0]['id'], first.id)

        response = self.client.get("{}?sort_by=-account".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data[0]['id'], second.id)

    def test_get_opportunities_dynamic_placement_rate_tech_fee_margin(self):
        today = timezone.now()
        start, end = today - timedelta(days=1), today + timedelta(days=1)
        opportunity = Opportunity.objects.create(
            id="1", name="A", start=start, end=end,
            probability=100)
        tech_fee = 0.12
        placement = OpPlacement.objects.create(
            opportunity=opportunity, tech_fee=tech_fee,
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
        self.assertAlmostEqual(response.data[0]['margin'], expected_margin)

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
        placement = OpPlacement.objects.create(opportunity=opportunity)
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
        opp_data = response.data[0]
        self.assertEqual(opp_data["video_view_rate"], 35.)

    def test_opportunity_margin_zero_cost(self):
        expected_margin = 100
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="A", start=today - timedelta(days=1),
            end=today + timedelta(days=1), probability=100
        )
        placement = OpPlacement.objects.create(
            goal_type_id=SalesForceGoalType.HARD_COST,
            opportunity=opportunity, total_cost=10)
        Campaign.objects.create(
            salesforce_placement=placement, cost=0)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        opp_data = response.data[0]
        self.assertEqual(opp_data["margin"], expected_margin)

    def test_opportunity_margin_zero_total_cost(self):
        today = timezone.now()
        opportunity = Opportunity.objects.create(
            id="1", name="A", start=today - timedelta(days=1),
            end=today + timedelta(days=1), probability=100
        )
        placement = OpPlacement.objects.create(
            goal_type_id=SalesForceGoalType.HARD_COST,
            opportunity=opportunity, total_cost=0)
        Flight.objects.create(placement=placement, start=today, end=today)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today,
                                         cost=1)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        opp_data = response.data[0]
        self.assertEqual(opp_data["margin"], -100)

    def test_opportunity_margin(self):
        today = timezone.now()
        campaign_1_cost = 10
        campaign_2_cost = 20
        placement_cpv_ordered_rate = 50
        campaign_cpv_video_views = 10
        hard_cost_client_cost = 400
        cost = campaign_1_cost + campaign_2_cost
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
                              total_cost=hard_cost_client_cost)
        Flight.objects.create(id="2", placement=placement_cpv,
                              start=today, end=today, total_cost=1000)
        campaign_1 = Campaign.objects.create(
            salesforce_placement=placement_hc, cost=campaign_1_cost)
        campaign_2 = Campaign.objects.create(
            id="2", salesforce_placement=placement_cpv,
            cost=campaign_2_cost)
        CampaignStatistic.objects.create(date=today,
                                         campaign=campaign_1,
                                         cost=campaign_1_cost)
        CampaignStatistic.objects.create(date=today,
                                         campaign=campaign_2,
                                         cost=campaign_2_cost,
                                         video_views=campaign_cpv_video_views)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        opp_data = response.data[0]
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
        self.assertEqual(len(response.data), 1)
        chart_data = response.data[0]["chart_data"]["cpv"]
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
        self.assertEqual(len(response.data), 1)
        chart_data = response.data[0]["chart_data"]["cpm"]
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
        self.assertEqual(len(response.data), 1)
        self.assertFalse(response.data[0]["has_dynamic_placements"])

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
        self.assertEqual(len(response.data), count)
        self.assertTrue(all(o["has_dynamic_placements"] for o in response.data))

    def test_dynamic_placement_budget(self):
        today = date(2017, 1, 1)
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
        expected_margin = (total_cost - aw_cost) / total_cost * 100
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
        self.assertEqual(len(response.data), 1)
        pl = response.data[0]
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
        self.assertAlmostEqual(pl["margin"], expected_margin)

    def test_no_dates(self):
        Opportunity.objects.create(
            id="1", name="1", start=None, end=None, probability=100
        )
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["status"], "undefined")
