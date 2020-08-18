# pylint: disable=too-many-lines
import logging
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from itertools import product
from urllib.parse import urlencode

import pytz
from django.contrib.auth import get_user_model
from django.db.models import Sum
from django.http import QueryDict
from django.urls import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED

from aw_reporting.api.views.pacing_report.constants import PACING_REPORT_OPPORTUNITIES_MAX_WATCH
from aw_reporting.api.urls.names import Name
from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Category
from aw_reporting.models import Flight
from aw_reporting.models import FlightStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import User
from aw_reporting.models.salesforce_constants import DYNAMIC_PLACEMENT_TYPES
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.unittests.generic_test import generic_test
from utils.unittests.int_iterator import int_iterator
from utils.unittests.patch_now import patch_now
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase

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
        pl_1 = OpPlacement.objects.create(id="1", name="", opportunity=current_op,
                                          goal_type_id=SalesForceGoalType.CPM)
        OpPlacement.objects.create(id="2", name="", opportunity=current_op,
                                   goal_type_id=SalesForceGoalType.CPV)
        Campaign.objects.create(name="c", salesforce_placement=pl_1)
        Opportunity.objects.create(id="2", name="2", start=month_ago,
                                   end=month_ago, probability=100)
        Opportunity.objects.create(id="3", name="3", start=month_after,
                                   end=month_after, probability=100)

        response = self.client.get("{}?period=this_month".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("watching", response.data)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        item = data[0]
        self.assertEqual(
            set(item.keys()),
            {
                "active_view_viewability",
                "ad_ops",
                "alerts",
                "am",
                "apex_deal",
                "aw_update_time",
                "billing_server",
                "cannot_roll_over",
                "category",
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
                "is_watched",
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
                "timezone",
                "video_completion_rates",
                "video_view_rate",
                "video_view_rate_quality",
                "video_views",
            }
        )
        self.assertEqual(item["id"], current_op.id)
        self.assertEqual(item["status"], "active")
        self.assertEqual(item["thumbnail"], ad_ops_user.profile_image_url)
        self.assertEqual(item["notes"], notes)
        self.assertEqual(item["category"]["id"], category.id)
        self.assertEqual(item["category"]["name"], category.name)
        self.assertEqual(item["region"]["id"], territory)
        self.assertEqual(item["region"]["name"], territory)

    def test_get_opportunities_buffers_default(self):
        today = timezone.now()
        first_day = today.replace(day=1)
        month_ago, _ = first_day - timedelta(
            days=1), first_day + timedelta(days=32)

        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   probability=100)
        Opportunity.objects.create(id="2", name="2", start=month_ago,
                                   end=month_ago, probability=100)
        response = self.client.get((self.url))
        self.assertEqual(HTTP_200_OK, response.status_code)

    def test_get_opportunities_filter_period(self):
        today = now_in_default_tz().date()
        first_day = today.replace(day=1)
        month_ago, month_after = first_day - timedelta(
            days=1), first_day + timedelta(days=32)

        opp_1 = Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                           probability=100)
        opp_2 = Opportunity.objects.create(id="2", name="2", start=month_ago,
                                           end=month_ago, probability=100)
        op_expected = Opportunity.objects.create(id="3", name="3",
                                                 start=month_after,
                                                 end=month_after,
                                                 probability=100)
        pl_1 = OpPlacement.objects.create(id=next(int_iterator), name="pl_1", opportunity=opp_1)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_1)
        pl_2 = OpPlacement.objects.create(name="pl_2", opportunity=opp_2)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_2)
        placement = OpPlacement.objects.create(id=next(int_iterator), name="pl", opportunity=op_expected)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=placement)

        response = self.client.get("{}?period=next_month".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], op_expected.id)

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
        placement = OpPlacement.objects.create(name="pl", opportunity=op_expected)
        Campaign.objects.create(salesforce_placement=placement)
        response = self.client.get("{}?ad_ops={}".format(self.url, user2.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], op_expected.id)

    def test_get_opportunities_filter_am(self):
        user1 = User.objects.create(id="1", name="1")
        user2 = User.objects.create(id="2", name="2")
        today = timezone.now()
        opp_1 = Opportunity.objects.create(id=next(int_iterator), name="1", start=today, end=today,
                                           account_manager=user1, probability=100)
        op_expected = Opportunity.objects.create(id=next(int_iterator), name="2",
                                                 start=today, end=today,
                                                 account_manager=user2,
                                                 probability=100)
        pl_1 = OpPlacement.objects.create(id=next(int_iterator), name="pl_1", opportunity=opp_1)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_1)
        placement = OpPlacement.objects.create(id=next(int_iterator), name="pl", opportunity=op_expected)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=placement)
        response = self.client.get("{}?am={}".format(self.url, user2.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], str(op_expected.id))

    def test_get_opportunities_filter_sales(self):
        user1 = User.objects.create(id="1", name="1")
        user2 = User.objects.create(id="2", name="2")
        today = timezone.now()
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   sales_manager=user1, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="2", start=today,
                                                 end=today, sales_manager=user2,
                                                 probability=100)
        placement = OpPlacement.objects.create(name="pl", opportunity=op_expected)
        Campaign.objects.create(name="c", salesforce_placement=placement)

        response = self.client.get("{}?sales={}".format(self.url, user2.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], op_expected.id)

    def test_get_opportunities_filter_category(self):
        category1 = Category.objects.create(id="Naive folks")
        category2 = Category.objects.create(id="Imagined bread")
        today = timezone.now()
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   category=category1, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="2", start=today,
                                                 end=today, category=category2,
                                                 probability=100)
        placement = OpPlacement.objects.create(name="pl", opportunity=op_expected)
        Campaign.objects.create(name="c", salesforce_placement=placement)
        response = self.client.get(
            "{}?category={}".format(self.url, category2.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], op_expected.id)

    def test_get_opportunities_filter_goal(self):
        today = timezone.now()
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   goal_type_id=SalesForceGoalType.CPM,
                                   probability=100)
        op_expected = Opportunity.objects.create(
            id="2", name="2", start=today, end=today,
            goal_type_id=SalesForceGoalType.CPV, probability=100)

        placement = OpPlacement.objects.create(name="pl", opportunity=op_expected)
        Campaign.objects.create(name="c", salesforce_placement=placement)

        response = self.client.get("{}?goal_type={}".format(self.url, 1))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], op_expected.id)

    def test_get_opportunities_filter_region(self):
        today = timezone.now()
        territory_1 = "Territory 1"
        territory_2 = "Territory 2"
        Opportunity.objects.create(id="1", name="1", start=today, end=today,
                                   territory=territory_1, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="2", start=today,
                                                 end=today, territory=territory_2,
                                                 probability=100)
        placement = OpPlacement.objects.create(name="pl", opportunity=op_expected)
        Campaign.objects.create(name="c", salesforce_placement=placement)

        query = QueryDict(mutable=True)
        query.update(region=territory_2)
        response = self.client.get("{}?{}".format(self.url, urlencode(query)))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], op_expected.id)

    def test_get_opportunities_filter_region_multiple(self):
        today = timezone.now()
        territory_1 = "Territory 1"
        territory_2 = "Territory 2"
        opp_1 = Opportunity.objects.create(id="1", name="1",
                                           start=today, end=today,
                                           territory=territory_1, probability=100)
        opp_2 = Opportunity.objects.create(id="2", name="2",
                                           start=today, end=today,
                                           territory=territory_2, probability=100)
        pl_1 = OpPlacement.objects.create(id=next(int_iterator), name="pl_1", opportunity=opp_1)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_1)
        pl_2 = OpPlacement.objects.create(id=next(int_iterator), name="pl_2", opportunity=opp_2)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_2)

        query = QueryDict(mutable=True)
        query.update(region=territory_1)
        query.update(region=territory_2)
        response = self.client.get("{}?{}".format(self.url, query.urlencode()))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 2)

    def test_get_opportunities_filter_name(self):
        today = timezone.now()
        opp_1 = Opportunity.objects.create(id="1", name="Peter Griffin", start=today,
                                           end=today, probability=100)
        op_expected = Opportunity.objects.create(id="2", name="Homer Simpson",
                                                 start=today, end=today,
                                                 probability=100)
        pl_1 = OpPlacement.objects.create(id=next(int_iterator), name="pl_1", opportunity=opp_1)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_1)

        placement = OpPlacement.objects.create(id=next(int_iterator), name="pl", opportunity=op_expected)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=placement)
        response = self.client.get("{}?search={}".format(self.url, "SIM"))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data["items"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["id"], op_expected.id)

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
        pl_1 = OpPlacement.objects.create(id=next(int_iterator), name="pl_1", opportunity=active)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_1)
        pl_2 = OpPlacement.objects.create(id=next(int_iterator), name="pl_2", opportunity=completed)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_2)
        pl_3 = OpPlacement.objects.create(id=next(int_iterator), name="pl_2", opportunity=upcoming)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_3)

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
        self.assertEqual(response.data["items"][0]["id"], active.id)

        response = self.client.get(
            "{}?{}".format(self.url,
                           urlencode(dict(status="upcoming", **filters))))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], upcoming.id)

        response = self.client.get(
            "{}?{}".format(self.url,
                           urlencode(dict(status="completed", **filters))))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], completed.id)

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
        pl_1 = OpPlacement.objects.create(id=next(int_iterator), name="pl_1", opportunity=first)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_1)
        pl_2 = OpPlacement.objects.create(id=next(int_iterator), name="pl_2", opportunity=second)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_2)

        response = self.client.get("{}?sort_by=-account".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 2)
        self.assertEqual(response.data["items"][0]["id"], second.id)
        self.assertEqual(response.data["items"][1]["id"], first.id)

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
        sum_cost, video_views = 123, 3214
        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(account=account, salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign, cost=sum_cost,
                                         video_views=video_views, date=start)
        recalculate_de_norm_fields_for_account(account.id)
        cpv = sum_cost / video_views
        expected_margin = tech_fee / (cpv + tech_fee) * 100
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertAlmostEqual(response.data["items"][0]["margin"],
                               expected_margin)

    def test_video_view_rate_shows_percents(self):
        """
        Bug: https://channelfactory.atlassian.net/browse/IQD-2701
        Summary: Pacing report > View rate parameter isn"t multiplied on 100%
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
        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(account=account,
                                           salesforce_placement=placement,
                                           video_views=1)
        CampaignStatistic.objects.create(date=today,
                                         campaign=campaign,
                                         video_views=35,
                                         impressions=100)
        recalculate_de_norm_fields_for_account(account.id)
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
        flight = Flight.objects.create(placement=placement,
                                       start=today, end=today,
                                       total_cost=1)
        Campaign.objects.create(name="c", salesforce_placement=placement)
        FlightStatistic.objects.create(flight=flight, sum_cost=0)
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

        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(account=account, salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today,
                                         cost=1)
        recalculate_de_norm_fields_for_account(account.id)

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
        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(
            id="2", account=account, salesforce_placement=placement_cpv,
            cost=campaign_cost)
        CampaignStatistic.objects.create(date=today,
                                         campaign=campaign,
                                         cost=campaign_cost,
                                         video_views=campaign_cpv_video_views)
        recalculate_de_norm_fields_for_account(account.id)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        opp_data = response.data["items"][0]
        self.assertEqual(opp_data["margin"], expected_margin)

    def test_dynamic_placement_negative(self):
        today = date(2017, 1, 1)
        opportunity = Opportunity.objects.create(probability=100,
                                                 start=today, end=today)

        placement = OpPlacement.objects.create(
            id=1,
            opportunity=opportunity,
            start=today - timedelta(days=1),
            end=today + timedelta(days=1)
        )
        Campaign.objects.create(name="c", salesforce_placement=placement)

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
            placement = OpPlacement.objects.create(
                id=index,
                opportunity=opportunity,
                dynamic_placement=dynamic_placement,
                start=start,
                end=end
            )
            Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=placement)
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
        recalculate_de_norm_fields_for_account(account.id)

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
        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(account=account,
                                           salesforce_placement=placement,
                                           video_views=1)
        CampaignStatistic.objects.create(date=today, campaign=campaign,
                                         cost=aw_cost,
                                         video_views=views,
                                         impressions=impressions)
        recalculate_de_norm_fields_for_account(account.id)
        # FlightStatistic.objects.create(flight=flight,
        #                                sum_cost=aw_cost,
        #                                video_views=views,
        #                                impressions=impressions
        #                                )
        with patch_now(today):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        pl = response.data["items"][0]
        self.assertAlmostEqual(pl["margin"], expected_margin)

    def test_no_dates(self):
        opp = Opportunity.objects.create(
            id="1", name="1", start=None, end=None, probability=100
        )
        placement = OpPlacement.objects.create(id=1, opportunity=opp)
        Campaign.objects.create(name="c", salesforce_placement=placement)
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
            "max_page": 1,
            "watching": 0,
            "max_watch": PACING_REPORT_OPPORTUNITIES_MAX_WATCH,
        })

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
        opp_3 = Opportunity.objects.create(id="3", name="3", start=today,
                                           end=today, category=category3,
                                           probability=100)

        pl_1 = OpPlacement.objects.create(id=next(int_iterator), name="pl_1", opportunity=opp_1)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_1)
        pl_2 = OpPlacement.objects.create(id=next(int_iterator), name="pl_2", opportunity=opp_2)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_2)
        pl_3 = OpPlacement.objects.create(id=next(int_iterator), name="pl_3", opportunity=opp_3)
        Campaign.objects.create(id=next(int_iterator), name="c", salesforce_placement=pl_3)

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

    def test_custom_date_range_filter(self):
        search_date = date(2018, 5, 22)
        opp_1 = Opportunity.objects.create(id=1, probability=100)
        opp_2 = Opportunity.objects.create(id=2, probability=100,
                                           start=search_date - timedelta(days=1),
                                           end=search_date - timedelta(days=1))
        opp_3 = Opportunity.objects.create(id=3, probability=100,
                                           start=search_date + timedelta(days=1),
                                           end=search_date + timedelta(days=1))
        opp_4 = Opportunity.objects.create(
            id=4, probability=100,
            start=search_date - timedelta(days=1),
            end=search_date + timedelta(days=1))
        placement_1 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opp_1, name="pl_1")
        Campaign.objects.create(id=next(int_iterator), name="c_1", salesforce_placement=placement_1)
        placement_2 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opp_2, name="pl_2")
        Campaign.objects.create(id=next(int_iterator), name="c_2", salesforce_placement=placement_2)
        placement_3 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opp_3, name="pl_3")
        Campaign.objects.create(id=next(int_iterator), name="c_1", salesforce_placement=placement_3)
        placement_4 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opp_4, name="pl_4")
        Campaign.objects.create(id=next(int_iterator), name="c_2", salesforce_placement=placement_4)
        opp_4.refresh_from_db()

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
        self.assertEqual(response.data["items"][0]["id"], opp_4.id)

    def test_ctr_multiply_by_100(self):
        any_date = date(2018, 1, 1)
        clicks, views, impressions = 234, 542, 654
        expected_ctr = clicks / impressions * 100
        expected_ctr_v = clicks / views * 100

        def create_opportunity(uid, goal_type_id, create_statistic=True):
            opportunity = Opportunity.objects.create(id=uid, probability=100)
            account = Account.objects.create(id=next(int_iterator))
            placement = OpPlacement.objects.create(id=uid,
                                                   opportunity=opportunity,
                                                   goal_type_id=goal_type_id)
            Flight.objects.create(id=uid, placement=placement, start=any_date,
                                  end=any_date)
            campaign = Campaign.objects.create(id=uid,
                                               account=account,
                                               salesforce_placement=placement,
                                               video_views=1)
            if create_statistic:
                CampaignStatistic.objects.create(date=any_date,
                                                 campaign=campaign,
                                                 clicks=clicks,
                                                 video_views=views,
                                                 impressions=impressions)
            recalculate_de_norm_fields_for_account(account.id)
            opportunity.refresh_from_db()
            return opportunity

        cpv_opportunity = create_opportunity(1, SalesForceGoalType.CPV)
        cpm_opportunity = create_opportunity(2, SalesForceGoalType.CPM)
        cpv_opportunity_no_statistic = create_opportunity(3, SalesForceGoalType.CPV, False)
        cpm_opportunity_no_statistic = create_opportunity(4, SalesForceGoalType.CPM, False)

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
        pl_1 = OpPlacement.objects.create(
            id=2, opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET)
        pl_2 = OpPlacement.objects.create(
            id=3, opportunity=opportunity,
            dynamic_placement=DynamicPlacementType.BUDGET)
        Campaign.objects.create(id=next(int_iterator), name="c_1", salesforce_placement=pl_1)
        Campaign.objects.create(id=next(int_iterator), name="c_2", salesforce_placement=pl_2)
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
        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(account=account, salesforce_placement=placement)
        CampaignStatistic.objects.create(date=today,
                                         campaign=campaign,
                                         cost=aw_cost)
        recalculate_de_norm_fields_for_account(account.id)
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
        placement_1 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity_1, name="pl_1")
        placement_2 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity_2, name="pl_2")
        Campaign.objects.create(id=next(int_iterator), name="c_1", salesforce_placement=placement_1)
        Campaign.objects.create(id=next(int_iterator), name="c_2", salesforce_placement=placement_2)
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
        Campaign.objects.create(name="c", salesforce_placement=hard_cost_placement)
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
        recalculate_de_norm_fields_for_account(account.id)
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
        opp = Opportunity.objects.create(id=next(int_iterator), probability=100)
        placement = OpPlacement.objects.create(name="pl_1", opportunity=opp)
        Campaign.objects.create(name="c", salesforce_placement=placement)
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

    @generic_test([
        ("True", (True,), dict()),
        ("False", (False,), dict()),
    ])
    def test_margin_cap_required(self, margin_cap_required):
        opp = Opportunity.objects.create(
            id=next(int_iterator),
            margin_cap_required=margin_cap_required,
            probability=100,
        )
        placement = OpPlacement.objects.create(name="p", opportunity=opp)
        Campaign.objects.create(name="c", salesforce_placement=placement)
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
        Campaign.objects.create(name="c", salesforce_placement=placement)
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
            id="1", name="1", start=today - timedelta(days=3), probability=100,
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

    def test_no_duplicate_stats_on_several_flights(self):
        flight_count = 2
        dates = [date(2019, 1, 1) + timedelta(days=i) for i in range(flight_count)]
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        account = Account.objects.create(id=next(int_iterator))
        placement = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM)
        for dt in dates:
            Flight.objects.create(id=next(int_iterator), start=dt, end=dt, placement=placement)
            campaign = Campaign.objects.create(id=next(int_iterator), account=account, salesforce_placement=placement,
                                               start_date=dt, end_date=dt)
            CampaignStatistic.objects.create(campaign=campaign, date=dt, impressions=1)

        recalculate_de_norm_fields_for_account(account.id)
        stats = FlightStatistic.objects.filter(flight__placement__opportunity=opportunity) \
            .aggregate(impressions=Sum("impressions"))

        response = self.client.get(self.url)
        self.assertEqual(stats["impressions"], response.data["items"][0]["impressions"])

    def test_no_duplicate_stats_on_single_flight(self):
        campaign_count = 2
        any_date = date(2019, 1, 1)
        opportunity = Opportunity.objects.create(id=next(int_iterator), probability=100)
        account = Account.objects.create(id=next(int_iterator))
        placement = OpPlacement.objects.create(
            id=next(int_iterator), opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM)
        Flight.objects.create(id=next(int_iterator), start=any_date, end=any_date, placement=placement)
        for _ in range(campaign_count):
            campaign = Campaign.objects.create(id=next(int_iterator), account=account, salesforce_placement=placement,
                                               start_date=any_date, end_date=any_date)
            CampaignStatistic.objects.create(campaign=campaign, date=any_date, impressions=1)

        recalculate_de_norm_fields_for_account(account.id)
        stats = FlightStatistic.objects.filter(flight__placement__opportunity=opportunity) \
            .aggregate(impressions=Sum("impressions"))

        response = self.client.get(self.url)
        self.assertEqual(stats["impressions"], response.data["items"][0]["impressions"])

    def test_exclude_opportunities_no_campaigns(self):
        any_date = date(2019, 1, 1)
        account = Account.objects.create(id=next(int_iterator))
        opp_1 = Opportunity.objects.create(id=next(int_iterator), probability=100)
        pl_1 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opp_1,
                                          goal_type_id=SalesForceGoalType.CPM)
        Flight.objects.create(id=next(int_iterator), start=any_date, end=any_date, placement=pl_1)
        Campaign.objects.create(id=next(int_iterator), account=account, salesforce_placement=pl_1, start_date=any_date,
                                end_date=any_date)

        Opportunity.objects.create(id=next(int_iterator), probability=100)
        pl_1 = OpPlacement.objects.create(id=next(int_iterator), opportunity=opp_1,
                                          goal_type_id=SalesForceGoalType.CPM)
        Flight.objects.create(id=next(int_iterator), start=any_date, end=any_date, placement=pl_1)

        response = self.client.get(self.url)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["id"], str(opp_1.id))
