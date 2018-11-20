import json
from datetime import timedelta, datetime, date

import pytz
from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from aw_reporting.models import SalesForceGoalType, Opportunity, OpPlacement, \
    Account, Campaign, AdGroup, GeoTarget, Category, CampaignStatistic, Topic, \
    TopicStatistic, AdGroupStatistic, Audience, AudienceStatistic, \
    VideoCreative, VideoCreativeStatistic, Genders, AgeRanges, \
    Flight, GeoTargeting, device_str, Device
from saas.urls.namespaces import Namespace
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.query import Operator
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase
from utils.utittests.patch_now import patch_now, int_iterator


class PricingToolTestCase(APITestCase):
    _url = reverse(
        Namespace.AW_REPORTING + ":" + Name.PricingTool.OPPORTUNITIES)

    def _request(self, **kwargs):
        return self.client.post(self._url,
                                json.dumps(kwargs),
                                content_type="application/json")

    def setUp(self):
        self.user = self.create_test_user()

    @staticmethod
    def _create_opportunity_campaign(_id, goal_type=SalesForceGoalType.CPV,
                                     opp_data=None, pl_data=None,
                                     camp_data=None, generate_statistic=True):
        today = timezone.now().date()
        period_days = 10
        start, end = today - timedelta(days=period_days), today
        default_opp_data = dict(start=start, end=end, brand="Test")
        opp_data = {**default_opp_data, **(opp_data or dict())}
        camp_data = camp_data or dict(name="Campaign name")
        pl_data = pl_data or dict(ordered_rate=0.6)
        opportunity = Opportunity.objects.create(id="opportunity_" + _id,
                                                 name="",
                                                 **opp_data)
        placement = OpPlacement.objects.create(
            id="op_placement_" + _id, name="", opportunity=opportunity,
            goal_type_id=goal_type,
            **pl_data)

        campaign = Campaign.objects.create(
            id="campaign_" + _id,
            salesforce_placement=placement, **camp_data
        )
        if generate_statistic:
            generate_campaign_statistic(campaign, opp_data["start"],
                                        opp_data["end"])
        return opportunity, campaign

    def test_pricing_tool_opportunity_returns_related_aw_campaigns(self):
        today = timezone.now().date()
        period_days = 10
        start, end = today - timedelta(days=period_days), today
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            salesforce_placement=placement
        )
        generate_campaign_statistic(campaign_1, start, end)

        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data['items']
        self.assertGreater(len(items), 0)
        opportunity_data = items[0]
        self.assertEqual(set(opportunity_data.keys()), {
            "id", "name", "brand", "vertical", "apex_deal", "campaigns",
            "products", "targeting", "demographic", "creative_lengths",
            "average_cpv", "average_cpm", "margin", "start_date", "end_date",
            "budget", "devices", "relevant_date_range", "sf_cpm", "sf_cpv",
            "geographic", "ctr", "ctr_v", "view_rate", "video100rate"
        })
        self.assertEqual(opportunity_data["id"], opportunity.id)
        campaigns_data = opportunity_data["campaigns"]
        self.assertEqual(len(campaigns_data), 1)
        campaign_data = campaigns_data[0]
        self.assertEqual(campaign_data["id"], campaign_1.id)
        self.assertEqual(set(campaign_data.keys()), {
            "id", "name", "thumbnail", "creative_lengths", "products",
            "devices", "demographic", "targeting", "cost", "average_cpm",
            "average_cpv", "margin", "relevant_date_range",
            "start_date", "end_date", "vertical", "geographic", "budget",
            "brand", "apex_deal", "sf_cpm", "sf_cpv", "ctr", "ctr_v"
        })

    def test_pricing_tool_opportunity_filters_by_quarter(self):
        start, end = datetime(2017, 10, 1), datetime(2017, 10, 11)
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        account = Account.objects.create(id="1", name="")

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="Campaign name", account=account,
            salesforce_placement=placement,
            start_date=start, end_date=end
        )
        generate_campaign_statistic(campaign_1, start, end)

        response = self._request(quarters=["Q1"])
        self.assertEqual(len(response.data["items"]), 0)

    def test_pricing_tool_opportunity_filters_by_product_type_single(self):
        start, end = datetime(2018, 1, 1), datetime(2018, 1, 11)
        type_1, type_2 = "Bumper", "NotBummer"
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 start=start, end=end,
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            salesforce_placement=placement,
        )
        AdGroup.objects.create(id="1", campaign=campaign_1, type=type_1)

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   start=start, end=end,
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )
        AdGroup.objects.create(id="2", campaign=campaign_2, type=type_2)
        generate_campaign_statistic(campaign_1, start, end)
        generate_campaign_statistic(campaign_2, start, end)

        response = self._request(product_types=[type_1])
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_product_type_or(self):
        start, end = datetime(2018, 1, 1), datetime(2018, 1, 11)
        type_1, type_2 = "Bumper", "NotBummer"
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 start=start, end=end,
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            salesforce_placement=placement,
        )
        AdGroup.objects.create(id="1", campaign=campaign_1, type=type_1)

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   start=start, end=end,
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )
        AdGroup.objects.create(id="2", campaign=campaign_2, type=type_2)
        generate_campaign_statistic(campaign_1, start, end)
        generate_campaign_statistic(campaign_2, start, end)

        response = self._request(product_types=[type_1, type_2],
                                 product_types_condition="or")
        self.assertEqual(len(response.data["items"]), 2)

    def test_pricing_tool_opportunity_filters_by_product_type_and(self):
        start, end = datetime(2018, 1, 1), datetime(2018, 1, 11)
        type_1, type_2 = "Bumper", "NotBummer"
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 start=start, end=end,
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            salesforce_placement=placement,
        )
        AdGroup.objects.create(id="1", campaign=campaign_1, type=type_1)
        AdGroup.objects.create(id="3", campaign=campaign_1, type=type_2)

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   start=start, end=end,
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )
        AdGroup.objects.create(id="2", campaign=campaign_2, type=type_2)
        generate_campaign_statistic(campaign_1, start, end)
        generate_campaign_statistic(campaign_2, start, end)

        response = self._request(product_types=[type_1, type_2],
                                 product_types_condition="and")
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_targeting_type(self):
        start, end = datetime(2018, 1, 1), datetime(2018, 1, 11)
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 start=start, end=end,
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            has_interests=True, salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   start=start, end=end,
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )

        response = self._request(targeting_types=["interests"])
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_targeting_type_or(self):
        start, end = datetime(2018, 1, 1), datetime(2018, 1, 11)
        type_1, type_2 = "Bumper", "NotBummer"
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 start=start, end=end,
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            has_interests=True, salesforce_placement=placement,
        )
        AdGroup.objects.create(id="1", campaign=campaign_1, type=type_1)

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   start=start, end=end,
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            has_keywords=True, salesforce_placement=placement_2,
        )
        AdGroup.objects.create(id="2", campaign=campaign_2, type=type_2)
        generate_campaign_statistic(campaign_1, start, end)
        generate_campaign_statistic(campaign_2, start, end)

        response = self._request(targeting_types=["interests", "keywords"],
                                 targeting_types_condition="OR")
        self.assertEqual(len(response.data["items"]), 2)

    def test_pricing_tool_opportunity_filters_by_targeting_type_and(self):
        start, end = datetime(2018, 1, 1), datetime(2018, 1, 11)
        type_1, type_2 = "Bumper", "NotBummer"
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 start=start, end=end,
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            has_interests=True, has_keywords=True,
            salesforce_placement=placement,
        )
        AdGroup.objects.create(id="1", campaign=campaign_1, type=type_1)

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   start=start, end=end,
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            has_keywords=True, salesforce_placement=placement_2,
        )
        AdGroup.objects.create(id="2", campaign=campaign_2, type=type_2)
        generate_campaign_statistic(campaign_1, start, end)
        generate_campaign_statistic(campaign_2, start, end)

        response = self._request(targeting_types=["interests", "keywords"],
                                 targeting_types_condition="AND")
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_gender(self):
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            gender_undetermined=True, salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        account_2 = Account.objects.create(id="2", name="")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name", account=account_2,
            salesforce_placement=placement_2,
        )

        response = self._request(genders=[0])
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_gender_or(self):
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            gender_undetermined=True, salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            gender_female=True, salesforce_placement=placement_2,
        )

        response = self._request(genders=[0, 1], demographic_condition="OR")
        self.assertEqual(len(response.data["items"]), 2)

    def test_pricing_tool_opportunity_filters_by_gender_and(self):
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            gender_undetermined=True, gender_female=True,
            salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            gender_female=True, salesforce_placement=placement_2,
        )

        response = self._request(genders=[0, 1], demographic_condition="AND")
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_age(self):
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            age_undetermined=True, salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )

        response = self._request(ages=[0])
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_age_or(self):
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            age_undetermined=True, salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            age_18_24=True, salesforce_placement=placement_2,
        )

        response = self._request(ages=[0, 1], demographic_condition="OR")
        self.assertEqual(len(response.data["items"]), 2)

    def test_pricing_tool_opportunity_filters_by_age_and(self):
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            age_undetermined=True, age_18_24=True,
            salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            age_18_24=True, salesforce_placement=placement_2,
        )

        response = self._request(ages=[0, 1], demographic_condition="AND")
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_parental_status(self):
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            parent_parent=True, salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )

        response = self._request(parents=[0])
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_parental_status_or(self):
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            parent_parent=True, salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            parent_not_parent=True, salesforce_placement=placement_2,
        )

        response = self._request(parents=[0, 1], demographic_condition="OR")
        self.assertEqual(len(response.data["items"]), 2)

    def test_pricing_tool_opportunity_filters_by_parental_status_and(self):
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="Campaign name",
            parent_parent=True, parent_not_parent=True,
            salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            parent_not_parent=True, salesforce_placement=placement_2,
        )

        response = self._request(parents=[0, 1], demographic_condition="AND")
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_geo_target(self):
        geo_target_defaults = dict(name="", canonical_name="", country_code="",
                                   target_type="", status="")
        geo_target_1, _ = GeoTarget.objects.get_or_create(
            id=111, defaults=geo_target_defaults,
        )
        geo_target_2, _ = GeoTarget.objects.get_or_create(
            id=222, defaults=geo_target_defaults,
        )
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="",
            salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )

        GeoTargeting.objects.create(campaign=campaign_1,
                                    geo_target=geo_target_1)
        GeoTargeting.objects.create(campaign=campaign_2,
                                    geo_target=geo_target_2)

        response = self._request(geo_locations=[geo_target_1.id])
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_geo_target_or(self):
        geo_target_defaults = dict(name="", canonical_name="", country_code="",
                                   target_type="", status="")
        geo_target_1, _ = GeoTarget.objects.get_or_create(
            id=111, defaults=geo_target_defaults,
        )
        geo_target_2, _ = GeoTarget.objects.get_or_create(
            id=222, defaults=geo_target_defaults,
        )
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="",
            salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )

        GeoTargeting.objects.create(campaign=campaign_1,
                                    geo_target=geo_target_1)
        GeoTargeting.objects.create(campaign=campaign_2,
                                    geo_target=geo_target_2)

        response = self._request(
            geo_locations=[geo_target_1.id, geo_target_2.id],
            geo_locations_condition="or")
        self.assertEqual(len(response.data["items"]), 2)

    def test_pricing_tool_opportunity_filters_by_geo_target_and(self):
        geo_target_defaults = dict(name="", canonical_name="", country_code="",
                                   target_type="", status="")
        geo_target_1, _ = GeoTarget.objects.get_or_create(
            id=111, defaults=geo_target_defaults,
        )
        geo_target_2, _ = GeoTarget.objects.get_or_create(
            id=222, defaults=geo_target_defaults,
        )
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="",
            salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )

        GeoTargeting.objects.create(campaign=campaign_1,
                                    geo_target=geo_target_1)
        GeoTargeting.objects.create(campaign=campaign_1,
                                    geo_target=geo_target_2)
        GeoTargeting.objects.create(campaign=campaign_2,
                                    geo_target=geo_target_2)

        response = self._request(
            geo_locations=[geo_target_1.id, geo_target_2.id],
            geo_locations_condition="and")
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_brand(self):
        brand = "Brand"
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand=brand)
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="",
            salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )

        response = self._request(brands=[brand])
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_brand_vertical(self):
        category, _ = Category.objects.get_or_create()
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test",
                                                 category=category)
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        Campaign.objects.create(
            id="campaign_1", name="",
            salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )

        response = self._request(categories=[category.id])
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_topics(self):
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="",
            salesforce_placement=placement,
        )

        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )
        ad_group_1 = AdGroup.objects.create(id="1", name="",
                                            campaign=campaign_1)
        ad_group_2 = AdGroup.objects.create(id="2", name="",
                                            campaign=campaign_1)

        ad_group_3 = AdGroup.objects.create(id="3", name="",
                                            campaign=campaign_2)
        ad_group_4 = AdGroup.objects.create(id="4", name="",
                                            campaign=campaign_2)
        top_topic_1 = Topic.objects.create(name="Music")
        child_topic_1 = Topic.objects.create(name="Classic",
                                             parent=top_topic_1)
        top_topic_2 = Topic.objects.create(name="Plants")
        child_topic_2 = Topic.objects.create(name="Vegetables",
                                             parent=top_topic_2)
        grand_child_topic_2 = Topic.objects.create(name="Tomato",
                                                   parent=child_topic_2)

        today = now_in_default_tz().date()
        TopicStatistic.objects.create(ad_group=ad_group_1, topic=child_topic_1,
                                      date=today)
        TopicStatistic.objects.create(ad_group=ad_group_2,
                                      topic=grand_child_topic_2, date=today)

        TopicStatistic.objects.create(ad_group=ad_group_3, topic=top_topic_2,
                                      date=today)
        TopicStatistic.objects.create(ad_group=ad_group_4, topic=child_topic_2,
                                      date=today)

        # test OR
        response = self._request(topics=[top_topic_1.id, top_topic_2.id],
                                 topics_condition="or")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 2)

        # test AND
        response = self._request(topics=[top_topic_1.id, top_topic_2.id],
                                 topics_condition="and")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filters_by_interests(self):
        today = now_in_default_tz().date()
        opportunity_1 = Opportunity.objects.create(id="opportunity_1",
                                                   name="", brand="Test")
        placement_1 = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity_1,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(id="1", name="",
                                             salesforce_placement=placement_1)
        ad_group_1 = AdGroup.objects.create(id="1", name="",
                                            campaign=campaign_1)
        ad_group_2 = AdGroup.objects.create(id="2", name="",
                                            campaign=campaign_1)
        opportunity_2 = Opportunity.objects.create(id="opportunity_2",
                                                   name="", brand="Test")
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(id="2", name="",
                                             salesforce_placement=placement_2)
        ad_group_3 = AdGroup.objects.create(id="3", name="",
                                            campaign=campaign_2)
        ad_group_4 = AdGroup.objects.create(id="4", name="",
                                            campaign=campaign_2)

        campaign_3 = Campaign.objects.create(id="3", name="")
        ad_group_5 = AdGroup.objects.create(id="5", name="",
                                            campaign=campaign_3)

        CampaignStatistic.objects.create(campaign=campaign_1, date=today)
        CampaignStatistic.objects.create(campaign=campaign_2, date=today)
        CampaignStatistic.objects.create(campaign=campaign_3, date=today)

        common = dict(average_position=1, date=today)
        AdGroupStatistic.objects.create(ad_group=ad_group_1, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_2, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_3, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_4, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_5, **common)

        top_interest_1 = Audience.objects.create(name="Music",
                                                 type=Audience.AFFINITY_TYPE)
        child_interest_1 = Audience.objects.create(name="Classic",
                                                   parent=top_interest_1,
                                                   type=Audience.AFFINITY_TYPE)
        top_interest_2 = Audience.objects.create(name="Plants",
                                                 type=Audience.AFFINITY_TYPE)
        child_interest_2 = Audience.objects.create(name="Vegetables",
                                                   parent=top_interest_2,
                                                   type=Audience.AFFINITY_TYPE)
        grand_interest_topic_2 = Audience.objects.create(
            name="Lol",
            parent=child_interest_2,
            type=Audience.AFFINITY_TYPE)

        AudienceStatistic.objects.create(ad_group=ad_group_1,
                                         audience=child_interest_1, date=today)
        AudienceStatistic.objects.create(ad_group=ad_group_2,
                                         audience=grand_interest_topic_2,
                                         date=today)

        AudienceStatistic.objects.create(ad_group=ad_group_3,
                                         audience=top_interest_2, date=today)
        AudienceStatistic.objects.create(ad_group=ad_group_4,
                                         audience=child_interest_2, date=today)

        # test OR
        response = self._request(
            interests=[top_interest_1.id, top_interest_2.id],
            interests_condition="or")
        self.assertEqual(response.status_code, HTTP_200_OK)
        opportunities = response.data["items"]
        self.assertEqual(set(c["id"] for c in opportunities),
                         {opportunity_1.id, opportunity_2.id})

        # test AND
        response = self._request(
            interests=[top_interest_1.id, top_interest_2.id],
            interests_condition="and")
        self.assertEqual(response.status_code, HTTP_200_OK)
        opportunities = response.data["items"]
        self.assertEqual(len(opportunities), 1)
        self.assertEqual(opportunities[0]["id"], opportunity_1.id)

    def test_pricing_tool_opportunity_filters_by_creative_length(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign("1")
        opportunity_2, campaign_2 = self._create_opportunity_campaign("2")
        self._create_opportunity_campaign("3")

        ad_group_1 = AdGroup.objects.create(id="1", name="",
                                            campaign=campaign_1)
        ad_group_2 = AdGroup.objects.create(id="2", name="",
                                            campaign=campaign_1)

        ad_group_3 = AdGroup.objects.create(id="3", name="",
                                            campaign=campaign_2)
        ad_group_4 = AdGroup.objects.create(id="4", name="",
                                            campaign=campaign_2)

        today = now_in_default_tz().date()
        common = dict(average_position=1, date=today)
        AdGroupStatistic.objects.create(ad_group=ad_group_1, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_2, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_3, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_4, **common)

        creative_1 = VideoCreative.objects.create(id="YYY",
                                                  duration=1000)  # 1sec
        creative_2 = VideoCreative.objects.create(id="XXX",
                                                  duration=60000)  # 60sec

        common = dict(impressions=1, date=today)
        VideoCreativeStatistic.objects.create(creative=creative_1,
                                              ad_group=ad_group_1, **common)
        VideoCreativeStatistic.objects.create(creative=creative_2,
                                              ad_group=ad_group_2, **common)

        VideoCreativeStatistic.objects.create(creative=creative_1,
                                              ad_group=ad_group_3, **common)

        response = self._request(creative_lengths=[0])
        self.assertEqual(len(response.data["items"]), 2)
        self.assertEqual(set([i["id"] for i in response.data["items"]]),
                         {opportunity_1.id, opportunity_2.id})

    def test_pricing_tool_opportunity_filters_by_creative_length_or(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign("1")
        opportunity_2, campaign_2 = self._create_opportunity_campaign("2")
        opportunity_3, campaign_3 = self._create_opportunity_campaign("3")

        ad_group_1 = AdGroup.objects.create(id="1", name="",
                                            campaign=campaign_1)
        ad_group_2 = AdGroup.objects.create(id="2", name="",
                                            campaign=campaign_1)

        ad_group_3 = AdGroup.objects.create(id="3", name="",
                                            campaign=campaign_2)
        ad_group_4 = AdGroup.objects.create(id="4", name="",
                                            campaign=campaign_2)

        ad_group_5 = AdGroup.objects.create(id="5", name="",
                                            campaign=campaign_3)
        today = now_in_default_tz().date()
        common = dict(average_position=1, date=today)
        AdGroupStatistic.objects.create(ad_group=ad_group_1, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_2, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_3, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_4, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_5, **common)

        creative_1 = VideoCreative.objects.create(id="YYY",
                                                  duration=1000)  # 1sec
        creative_2 = VideoCreative.objects.create(id="XXX",
                                                  duration=60000)  # 60sec

        common = dict(impressions=1, date=today)
        VideoCreativeStatistic.objects.create(creative=creative_1,
                                              ad_group=ad_group_1, **common)
        VideoCreativeStatistic.objects.create(creative=creative_2,
                                              ad_group=ad_group_2, **common)

        VideoCreativeStatistic.objects.create(creative=creative_1,
                                              ad_group=ad_group_3, **common)

        response = self._request(creative_lengths=[0, 4],
                                 creative_lengths_condition="or")
        self.assertEqual(len(response.data["items"]), 2)
        self.assertEqual(set([i["id"] for i in response.data["items"]]),
                         {opportunity_1.id, opportunity_2.id})

    def test_pricing_tool_opportunity_filters_by_creative_length_and(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign("1")
        opportunity_2, campaign_2 = self._create_opportunity_campaign("2")
        opportunity_3, campaign_3 = self._create_opportunity_campaign("3")

        ad_group_1 = AdGroup.objects.create(id="1", name="",
                                            campaign=campaign_1)
        ad_group_2 = AdGroup.objects.create(id="2", name="",
                                            campaign=campaign_1)

        ad_group_3 = AdGroup.objects.create(id="3", name="",
                                            campaign=campaign_2)
        ad_group_4 = AdGroup.objects.create(id="4", name="",
                                            campaign=campaign_2)

        ad_group_5 = AdGroup.objects.create(id="5", name="",
                                            campaign=campaign_3)
        today = now_in_default_tz().date()
        common = dict(average_position=1, date=today)
        AdGroupStatistic.objects.create(ad_group=ad_group_1, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_2, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_3, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_4, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_5, **common)

        creative_1 = VideoCreative.objects.create(id="YYY",
                                                  duration=1000)  # 1sec
        creative_2 = VideoCreative.objects.create(id="XXX",
                                                  duration=60000)  # 60sec

        common = dict(impressions=1, date=today)
        VideoCreativeStatistic.objects.create(creative=creative_1,
                                              ad_group=ad_group_1, **common)
        VideoCreativeStatistic.objects.create(creative=creative_2,
                                              ad_group=ad_group_2, **common)

        VideoCreativeStatistic.objects.create(creative=creative_1,
                                              ad_group=ad_group_3, **common)

        response = self._request(creative_lengths=[0, 4],
                                 creative_lengths_condition="and")
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity_1.id)

    def test_pricing_tool_opportunity_filter_devices(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", camp_data=dict(device_computers=True, device_mobile=True))
        opportunity_2, campaign_2 = self._create_opportunity_campaign(
            "2", camp_data=dict(device_computers=True, device_tablets=True))
        self._create_opportunity_campaign(
            "3", camp_data=dict(device_other=True))

        # test OR
        response = self._request(devices=[1, 2],
                                 devices_condition="or")
        self.assertEqual(response.status_code, HTTP_200_OK)
        opportunities = response.data["items"]
        self.assertEqual(set(c["id"] for c in opportunities),
                         {opportunity_1.id, opportunity_2.id})

        # test AND
        response = self._request(devices=[0, 1],
                                 devices_condition="and")
        self.assertEqual(response.status_code, HTTP_200_OK)
        opportunities = response.data["items"]
        self.assertEqual(len(opportunities), 1)
        self.assertEqual(opportunities[0]["id"], opportunity_1.id)

    def test_pricing_tool_opportunity_filter_apex(self):
        opportunity, _ = self._create_opportunity_campaign(
            "1", opp_data=dict(apex_deal=True))
        self._create_opportunity_campaign("2")

        response = self._request(apex_deal="1")

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_filter_ctr(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False)
        opportunity_2, campaign_2 = self._create_opportunity_campaign(
            "2", generate_statistic=False)

        CampaignStatistic.objects.create(campaign=campaign_1,
                                         date=date(2017, 1, 1),
                                         clicks=20,
                                         impressions=1000)
        CampaignStatistic.objects.create(campaign=campaign_2,
                                         date=date(2017, 1, 1),
                                         clicks=40,
                                         impressions=1000)

        response = self._request(max_ctr=3., min_ctr=1.5)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data["items"]), 1)
        self.assertEqual(data["items"][0]["id"], opportunity_1.id)

    def test_pricing_tool_opportunity_filter_ctr_v(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False)
        opportunity_2, campaign_2 = self._create_opportunity_campaign(
            "2", generate_statistic=False)

        CampaignStatistic.objects.create(campaign=campaign_1,
                                         date=date(2017, 1, 1),
                                         clicks=20,
                                         video_views=1000)
        CampaignStatistic.objects.create(campaign=campaign_2,
                                         date=date(2017, 1, 1),
                                         clicks=40,
                                         video_views=1000)
        response = self._request(max_ctr_v=3., min_ctr_v=1.5)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data["items"]), 1)
        self.assertEqual(data["items"][0]["id"], opportunity_1.id)

    def test_pricing_tool_opportunity_filter_view_rate(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", camp_data=dict(clicks=20, video_views=200, impressions=1000),
            generate_statistic=False)
        _, campaign_2 = self._create_opportunity_campaign(
            "2", camp_data=dict(clicks=40, video_views=400, impressions=1000),
            generate_statistic=False)
        CampaignStatistic.objects.create(campaign=campaign_1,
                                         date=date(2017, 1, 1),
                                         video_views=200,
                                         impressions=1000)
        CampaignStatistic.objects.create(campaign=campaign_2,
                                         date=date(2017, 1, 1),
                                         video_views=400,
                                         impressions=1000)

        response = self._request(min_video_view_rate=10,
                                 max_video_view_rate=30)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data["items"]), 1)
        self.assertEqual(data["items"][0]["id"], opportunity_1.id)

    def test_pricing_tool_opportunity_filter_by_video100rate(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False)
        _, campaign_2 = self._create_opportunity_campaign(
            "2", generate_statistic=False)

        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign_1,
                                         video_views_100_quartile=20,
                                         impressions=100)
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign_2,
                                         video_views_100_quartile=40,
                                         impressions=100)
        response = self._request(min_video100rate=10,
                                 max_video100rate=30)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data["items"]), 1)
        self.assertEqual(data["items"][0]["id"], opportunity_1.id)

    def test_filter_by_video100rate_relates_to_date_range(self):
        date_filter = date(2017, 1, 1)
        _, campaign = self._create_opportunity_campaign(
            "1", generate_statistic=False)
        CampaignStatistic.objects.create(date=date_filter,
                                         campaign=campaign,
                                         video_views_100_quartile=1,
                                         impressions=10)
        CampaignStatistic.objects.create(date=date_filter - timedelta(days=1),
                                         campaign=campaign,
                                         video_views_100_quartile=999,
                                         impressions=999)
        response = self._request(start=str(date_filter),
                                 end=str(date_filter),
                                 min_video100rate=10,
                                 max_video100rate=10)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertAlmostEqual(response.data["items"][0]["video100rate"], 10.)

    def test_pricing_tool_opportunity_aggregated_devices(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        _, campaign = self._create_opportunity_campaign(
            "1", opp_data=dict(start=start_date, end=end_date))
        campaign.device_computers = True
        campaign.device_tablets = True
        campaign.save()
        expected_devices = {device_str(Device.COMPUTER), device_str(Device.TABLET)}

        response = self._request(start=str(start_date), end=str(end_date))

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        opportunities = data["items"]
        self.assertEqual(len(opportunities), 1)
        opp_data = opportunities[0]
        self.assertEqual(opp_data["devices"], expected_devices)

    def test_pricing_tool_opportunity_aggregated_products(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        opportunity, campaign = self._create_opportunity_campaign(
            "1", opp_data=dict(start=start_date, end=end_date))
        test_type_1 = "test_type_1"
        test_type_2 = "test_type_2"
        expected_products = {test_type_1, test_type_2}
        AdGroup.objects.create(id="1", campaign=campaign, type=test_type_1)
        AdGroup.objects.create(id="2", campaign=campaign, type=test_type_2)

        response = self._request(start=str(start_date), end=str(end_date))

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        opportunities = data["items"]
        self.assertEqual(len(opportunities), 1)
        opp_data = opportunities[0]
        self.assertEqual(set(opp_data["products"]), expected_products)

    def test_pricing_tool_opportunity_aggregated_targeting(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        opportunity, campaign = self._create_opportunity_campaign(
            "1", opp_data=dict(start=start_date, end=end_date))

        campaign.has_interests = True
        campaign.has_videos = True
        campaign.save()
        expected_targeting = {"interests", "videos"}

        response = self._request(start=str(start_date), end=str(end_date))

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        opportunities = data["items"]
        self.assertEqual(len(opportunities), 1)
        opp_data = opportunities[0]
        self.assertEqual(set(opp_data["targeting"]), expected_targeting)

    def test_pricing_tool_opportunity_aggregated_demographic(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        opportunity, campaign = self._create_opportunity_campaign(
            "1", opp_data=dict(start=start_date, end=end_date))

        campaign.age_18_24 = True
        campaign.gender_female = True
        campaign.save()
        expected_demographic = {"18-24", "Female"}

        response = self._request(start=str(start_date), end=str(end_date))

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        opportunities = data["items"]
        self.assertEqual(len(opportunities), 1)
        opp_data = opportunities[0]
        self.assertEqual(set(opp_data["demographic"]), expected_demographic)

    def test_success_response_on_not_enough_data(self):
        opportunity = Opportunity.objects.create(id="AAA")
        placement = OpPlacement.objects.create(opportunity=opportunity)
        Campaign.objects.create(salesforce_placement=placement)
        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_pricing_tool_opportunity_aggregated_creative_length(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        opportunity, campaign = self._create_opportunity_campaign(
            "1", opp_data=dict(start=start_date, end=end_date))
        ad_group = AdGroup.objects.create(id="1", campaign=campaign)
        creative_duration = 123
        creative = VideoCreative.objects.create(duration=creative_duration)
        VideoCreativeStatistic.objects.create(ad_group=ad_group,
                                              creative=creative,
                                              date=start_date)

        response = self._request(start=str(start_date), end=str(end_date))

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        opportunities = data["items"]
        self.assertEqual(len(opportunities), 1)
        opp_data = opportunities[0]
        self.assertEqual(opp_data["creative_lengths"], [creative_duration])

    def test_pricing_tool_opportunity_aggregated_cpv_cpm(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        views_costs = [23, 74]
        views = [243, 773]
        opportunity, campaign_1 = self._create_opportunity_campaign(
            "1", opp_data=dict(start=start_date, end=end_date),
            goal_type=SalesForceGoalType.CPV,
            generate_statistic=False)
        campaign_2 = Campaign.objects.create(
            id="2", salesforce_placement=campaign_1.salesforce_placement)

        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM, ordered_rate=0.6)
        account = Account.objects.create(id="2", name="")

        impressions_costs = [112, 832]
        impressions = [1132, 32451]
        campaign_3 = Campaign.objects.create(
            id="3", name="", account=account,
            salesforce_placement=placement_2)
        campaign_4 = Campaign.objects.create(
            id="4", name="", account=account,
            salesforce_placement=placement_2)

        CampaignStatistic.objects.create(date=start_date,
                                         campaign=campaign_1,
                                         cost=views_costs[0],
                                         video_views=views[0])
        CampaignStatistic.objects.create(date=start_date,
                                         campaign=campaign_2,
                                         cost=views_costs[1],
                                         video_views=views[1])
        CampaignStatistic.objects.create(date=start_date,
                                         campaign=campaign_3,
                                         cost=impressions_costs[0],
                                         impressions=impressions[0])
        CampaignStatistic.objects.create(date=start_date,
                                         campaign=campaign_4,
                                         cost=impressions_costs[1],
                                         impressions=impressions[1])
        expected_cpv = sum(views_costs) / sum(views)
        expected_cpm = sum(impressions_costs) / sum(impressions) * 1000

        response = self._request(start=str(start_date), end=str(end_date))

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        opportunities = data["items"]
        self.assertEqual(len(opportunities), 1)
        opp_data = opportunities[0]

        self.assertEqual(opp_data["average_cpv"], expected_cpv)
        self.assertEqual(opp_data["average_cpm"], expected_cpm)

    def test_pricing_tool_opportunity_cpv_cpm_client_rate(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        cpm_cost, cpv_cost = 3423, 3245
        cpm_units, cpv_units = 124543, 23435
        opp_cpm = cpm_cost / cpm_units * 1000
        opp_cpv = cpv_cost / cpv_units
        opportunity, _ = self._create_opportunity_campaign(
            "1",
            opp_data=dict(start=start_date, end=end_date),
            pl_data=dict(total_cost=cpv_cost,
                         start=start_date, end=end_date,
                         ordered_units=cpv_units),
            goal_type=SalesForceGoalType.CPV)

        OpPlacement.objects.create(id="2", opportunity=opportunity,
                                   start=start_date, end=end_date,
                                   goal_type_id=SalesForceGoalType.CPM,
                                   ordered_units=cpm_units,
                                   total_cost=cpm_cost)

        response = self._request(start=str(start_date), end=str(end_date))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        opportunity_data = response.data["items"][0]
        self.assertEqual(opportunity_data["sf_cpm"], opp_cpm)
        self.assertEqual(opportunity_data["sf_cpv"], opp_cpv)

    def test_pricing_tool_campaign_cpv_client_rate(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        pl_rate = 1.03
        opportunity, _ = self._create_opportunity_campaign(
            "1",
            opp_data=dict(start=start_date, end=end_date),
            pl_data=dict(ordered_rate=pl_rate,
                         start=start_date, end=end_date),
            goal_type=SalesForceGoalType.CPV)

        response = self._request(start=str(start_date), end=str(end_date))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        self.assertEqual(campaign_data["sf_cpv"], pl_rate)
        self.assertEqual(campaign_data["sf_cpm"], None)

    def test_pricing_tool_campaign_cpm_client_rate(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        pl_rate = 1.03
        self._create_opportunity_campaign(
            "1",
            opp_data=dict(start=start_date, end=end_date),
            pl_data=dict(ordered_rate=pl_rate),
            goal_type=SalesForceGoalType.CPM)

        response = self._request(start=str(start_date), end=str(end_date))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        placement_data = response.data["items"][0]["campaigns"][0]
        self.assertEqual(placement_data["sf_cpv"], None)
        self.assertEqual(placement_data["sf_cpm"], pl_rate)

    def test_hides_opportunity_with_no_placement(self):
        today = timezone.now().date()
        period_days = 10
        start, end = today - timedelta(days=period_days), today
        Opportunity.objects.create(id="opportunity",
                                   name="", brand="Test")
        response = self._request(start=str(start), end=str(end))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 0)
        self.assertEqual(len(response.data["items"]), 0)

    def test_hides_opportunity_with_no_matched_campaign(self):
        today = timezone.now().date()
        period_days = 10
        start, end = today - timedelta(days=period_days), today
        opportunity = Opportunity.objects.create(id="opportunity",
                                                 name="", brand="Test")
        OpPlacement.objects.create(
            id="op_placement", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        response = self._request(start=str(start), end=str(end))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 0)
        self.assertEqual(len(response.data["items"]), 0)

    def test_no_opportunity_duplicates(self):
        """
        Bug: https://channelfactory.atlassian.net/browse/IQD-2676
        Summary: Pricing tool > User see only two opportunity cards
                 instead of 10
        Root cause: filter by product_types duplicates entities
        """
        opportunity = Opportunity.objects.create(id="opportunity",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        c1 = Campaign.objects.create(id="1", salesforce_placement=placement)
        c2 = Campaign.objects.create(id="2", salesforce_placement=placement)
        test_ad_group_type = "Video discovery"
        AdGroup.objects.create(id="1", campaign=c1, type=test_ad_group_type)
        AdGroup.objects.create(id="2", campaign=c2, type=test_ad_group_type)
        response = self._request(product_types=[test_ad_group_type])

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)

    def test_dates_on_opportunity_level(self):
        """
        https://channelfactory.atlassian.net/browse/IQD-2679
        > Start-End Dates on opportunity level = min AW start and max end date
        withing campaigns
        """
        start_1, end_1 = datetime(2018, 1, 1), datetime(2018, 1, 10)
        start_2, end_2 = datetime(2018, 1, 12), datetime(2018, 1, 30)
        opportunity = Opportunity.objects.create(id="opportunity",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects \
            .create(id="1", salesforce_placement=placement,
                    start_date=start_1, end_date=end_1)
        Campaign.objects \
            .create(id="2", salesforce_placement=placement,
                    start_date=start_2, end_date=end_2)
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        opp_data = response.data["items"][0]
        self.assertEqual(opp_data["start_date"], start_1.date())
        self.assertEqual(opp_data["end_date"], end_2.date())

    def test_dates_on_company_level(self):
        """
        https://channelfactory.atlassian.net/browse/IQD-2679
        > Start-End Dates on campaign level = AW start and end date
        """
        start_1, end_1 = date(2018, 1, 1), date(2018, 1, 10)
        start_2, end_2 = date(2018, 1, 12), date(2018, 1, 30)
        opportunity = Opportunity.objects.create(id="opportunity",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_1 = Campaign.objects \
            .create(id="1", salesforce_placement=placement,
                    start_date=start_1, end_date=end_1)
        campaign_2 = Campaign.objects \
            .create(id="2", salesforce_placement=placement,
                    start_date=start_2, end_date=end_2)
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        campaigns = response.data["items"][0]["campaigns"]
        company_1_data = [c for c in campaigns if c["id"] == campaign_1.id][0]
        company_2_data = [c for c in campaigns if c["id"] == campaign_2.id][0]
        self.assertEqual(company_1_data["start_date"], start_1)
        self.assertEqual(company_1_data["end_date"], end_1)
        self.assertEqual(company_2_data["start_date"], start_2)
        self.assertEqual(company_2_data["end_date"], end_2)

    def test_relevant_date_range_start_end(self):
        start, end = datetime(2015, 12, 1), datetime(2016, 1, 30)
        request_start, request_end = date(2016, 1, 5), date(2016, 1, 20)
        opportunity = Opportunity.objects.create(id="opportunity",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects \
            .create(id="1", salesforce_placement=placement,
                    start_date=start, end_date=end)

        response = self._request(start=str(request_start), end=str(request_end))
        self.assertEqual(response.data["items"][0]["relevant_date_range"],
                         dict(start=request_start, end=request_end))

    def test_relevant_date_range_quarters(self):
        now = datetime(2016, 2, 1)
        start, end = datetime(2015, 12, 1), datetime(2016, 1, 30)
        start_of_the_year = date(2016, 1, 1)
        opportunity = Opportunity.objects.create(id="opportunity",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        Campaign.objects \
            .create(id="1", salesforce_placement=placement,
                    start_date=start, end_date=end)

        with patch_now(now):
            response = self._request(quarters=["Q1"])
        self.assertEqual(response.data["items"][0]["relevant_date_range"],
                         dict(start=start_of_the_year, end=end.date()))

    def test_budget_on_campaign_level(self):
        """
        https://channelfactory.atlassian.net/browse/IQD-2679
        > Budget on opportunity level = SUM AW cost from all campaigns
        which belong to the opportunity
        """
        budget_1 = 123
        budget_2 = 234
        opportunity = Opportunity.objects.create(id="opportunity",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_1 = Campaign.objects \
            .create(id="1", salesforce_placement=placement, cost=budget_1)
        campaign_2 = Campaign.objects \
            .create(id="2", salesforce_placement=placement, cost=budget_2)
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        campaigns = response.data["items"][0]["campaigns"]
        company_1_data = [c for c in campaigns if c["id"] == campaign_1.id][0]
        company_2_data = [c for c in campaigns if c["id"] == campaign_2.id][0]
        self.assertEqual(company_1_data["budget"], budget_1)
        self.assertEqual(company_2_data["budget"], budget_2)

    def test_budget_on_opportunity_level(self):
        """
        https://channelfactory.atlassian.net/browse/IQD-2679
        > Budget on opportunity level = SUM AW cost from all campaigns
        which belong to the opportunity
        """
        budget_1 = 123
        budget_2 = 234
        opportunity = Opportunity.objects.create(id="opportunity",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_1 = Campaign.objects \
            .create(id="1", salesforce_placement=placement)
        campaign_2 = Campaign.objects \
            .create(id="2", salesforce_placement=placement)
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign_1,
                                         cost=budget_1)
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign_2,
                                         cost=budget_2)
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        opportunity_data = response.data["items"][0]
        self.assertEqual(opportunity_data["budget"], budget_1 + budget_2)

    def test_opportunity_has_geo_target(self):
        test_geo_1 = "test geo 1"
        test_geo_2 = "test geo 2"
        geo_target_defaults = dict(canonical_name="", country_code="",
                                   target_type="", status="")
        geo_target_1, _ = GeoTarget.objects.get_or_create(
            id=111, name=test_geo_1, defaults=geo_target_defaults,
        )
        geo_target_2, _ = GeoTarget.objects.get_or_create(
            id=222, name=test_geo_2, defaults=geo_target_defaults,
        )
        opportunity = Opportunity.objects.create(id="opportunity_1",
                                                 name="", brand="Test")
        placement = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)

        campaign_1 = Campaign.objects.create(
            id="campaign_1", name="",
            salesforce_placement=placement,
        )

        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        campaign_2 = Campaign.objects.create(
            id="campaign_2", name="Campaign name",
            salesforce_placement=placement_2,
        )

        campaign_3 = Campaign.objects.create(
            id="campaign_3", name="Campaign name 3",
            salesforce_placement=placement_2
        )

        GeoTargeting.objects.create(campaign=campaign_1,
                                    geo_target=geo_target_1)
        GeoTargeting.objects.create(campaign=campaign_2,
                                    geo_target=geo_target_1)
        GeoTargeting.objects.create(campaign=campaign_2,
                                    geo_target=geo_target_2)
        response = self._request()
        self.assertEqual(len(response.data["items"]), 1)
        opp_data = response.data["items"][0]
        actual_geo = opp_data["geographic"]
        self.assertEqual(len(actual_geo), 2)
        self.assertEqual(set(actual_geo), {test_geo_1, test_geo_2})
        campaigns = opp_data["campaigns"]
        campaign_1_data = [c for c in campaigns if c["id"] == campaign_1.id][0]
        campaign_2_data = [c for c in campaigns if c["id"] == campaign_2.id][0]
        campaign_3_data = [c for c in campaigns if c["id"] == campaign_3.id][0]
        self.assertEqual(campaign_1_data["geographic"], [test_geo_1])
        self.assertEqual(set(campaign_2_data["geographic"]),
                         {test_geo_1, test_geo_2})
        self.assertEqual(campaign_3_data["geographic"], [])

    def test_campaign_cpm_cpv(self):
        _, campaign = self._create_opportunity_campaign(
            "1", camp_data=dict(cost=123, impressions=2234, video_views=432))
        expected_cpm = campaign.cost / campaign.impressions * 1000
        expected_cpv = campaign.cost / campaign.video_views
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        self.assertEqual(campaign_data["average_cpm"], expected_cpm)
        self.assertEqual(campaign_data["average_cpv"], expected_cpv)

    def test_filter_by_video100rate_no_crash(self):
        """
        Bug: https://channelfactory.atlassian.net/browse/IQD-2693
        Summary: Pricing tool > 500 server error when user try to set
        min > 30% IN filter by "100% completion rate" and "View Rate"
        Root cause: filter multiply rate by impressions count and overheads
            Integer field
        """
        _, campaign = self._create_opportunity_campaign("1")
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign,
                                         video_views_100_quartile=1947483646,
                                         impressions=2147483646)
        response = self._request(min_video100rate=75)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)

    def test_filter_by_view_rate_no_crash(self):
        _, campaign = self._create_opportunity_campaign("1")
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign,
                                         video_views=1147483646,
                                         impressions=2147483646)
        response = self._request(max_video_view_rate=75)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)

    def test_filters_by_creative_length_includes_upper_bound(self):
        """
        Task: https://channelfactory.atlassian.net/browse/IQD-2692
        Summary: Pricing tool > Creative length borders improvement
        """
        opportunity_1, campaign_1 = self._create_opportunity_campaign("1")
        opportunity_2, campaign_2 = self._create_opportunity_campaign("2")
        self._create_opportunity_campaign("3")

        ad_group_1 = AdGroup.objects.create(id="1", name="",
                                            campaign=campaign_1)
        ad_group_2 = AdGroup.objects.create(id="2", name="",
                                            campaign=campaign_2)

        today = now_in_default_tz().date()
        common = dict(average_position=1, date=today)
        AdGroupStatistic.objects.create(ad_group=ad_group_1, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_2, **common)

        creative_1 = VideoCreative.objects.create(id="YYY",
                                                  duration=6000)  # 1sec
        creative_2 = VideoCreative.objects.create(id="XXX",
                                                  duration=60000)  # 60sec

        common = dict(impressions=1, date=today)
        VideoCreativeStatistic.objects.create(creative=creative_1,
                                              ad_group=ad_group_1, **common)
        VideoCreativeStatistic.objects.create(creative=creative_2,
                                              ad_group=ad_group_2, **common)
        response = self._request(creative_lengths=[0])
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(set([i["id"] for i in response.data["items"]]),
                         {opportunity_1.id})

    def test_filter_by_ctr_hides_opportunities_without_impressions(self):
        """
        Bug: https://channelfactory.atlassian.net/browse/IQD-2694
        Summary: Pricing tool > CTR(v) and CTR(i) filters return
                not valid set of data
        Root cause: multiplying by zero
        """
        self._create_opportunity_campaign("1", camp_data=dict(impressions=0))
        response = self._request(min_ctr=1)
        self.assertEqual(len(response.data["items"]), 0)

    def test_filter_by_ctr_v_hides_opportunities_without_ctr_views(self):
        """
        Bug: https://channelfactory.atlassian.net/browse/IQD-2694
        Summary: Pricing tool > CTR(v) and CTR(i) filters return
                not valid set of data
        Root cause: multiplying by zero
        """
        self._create_opportunity_campaign("1", camp_data=dict(video_views=0))
        response = self._request(min_ctr_v=1)
        self.assertEqual(len(response.data["items"]), 0)

    def test_filter_by_video_rate_hides_opportunities_without_impressions(self):
        self._create_opportunity_campaign("1", generate_statistic=False)
        response = self._request(min_video_view_rate=1)
        self.assertEqual(len(response.data["items"]), 0)

    def test_filter_by_video100rate_hides_opportunities_without_impressions(
            self):
        self._create_opportunity_campaign(
            "1", camp_data=dict(impressions=0, video_views_100_quartile=0))
        response = self._request(min_video100rate=1)
        self.assertEqual(len(response.data["items"]), 0)

    def test_campaign_name(self):
        test_name = "Campaign Name"
        self._create_opportunity_campaign("1", camp_data=dict(name=test_name))
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        self.assertEqual(campaign_data["name"], test_name)

    def test_campaign_apex_deal_true(self):
        self._create_opportunity_campaign("1", opp_data=dict(apex_deal=True))
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        self.assertTrue(campaign_data["apex_deal"])

    def test_campaign_apex_deal_false(self):
        self._create_opportunity_campaign("1", opp_data=dict(apex_deal=False))
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        self.assertFalse(campaign_data["apex_deal"])

    def test_campaign_brand(self):
        test_brand = "Test brand 1123"
        self._create_opportunity_campaign("1", opp_data=dict(brand=test_brand))
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        self.assertEqual(campaign_data["brand"], test_brand)

    def test_campaign_devices(self):
        self._create_opportunity_campaign(
            "1", camp_data=dict(device_computers=True, device_tablets=True))
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        self.assertEqual(set(campaign_data["devices"]),
                         {device_str(Device.COMPUTER), device_str(Device.TABLET)})

    def test_campaign_products(self):
        _, campaign = self._create_opportunity_campaign(
            "1", camp_data=dict(device_computers=True, device_tablets=True))
        test_type_1 = "test_type_1"
        test_type_2 = "test_type_2"
        expected_products = {test_type_1, test_type_2}
        AdGroup.objects.create(id="1", campaign=campaign, type=test_type_1)
        AdGroup.objects.create(id="2", campaign=campaign, type=test_type_2)
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        self.assertEqual(set(campaign_data["products"]), expected_products)

    def test_campaign_targeting(self):
        self._create_opportunity_campaign(
            "1", camp_data=dict(has_interests=True, has_remarketing=True))
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        expected_targeting = {"interests", "remarketing"}
        self.assertEqual(set(campaign_data["targeting"]), expected_targeting)

    def test_campaign_demographic(self):
        self._create_opportunity_campaign(
            "1", camp_data=dict(age_18_24=True, gender_female=True))
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        expected_demographic = {AgeRanges[1], Genders[1]}
        self.assertEqual(set(campaign_data["demographic"]),
                         expected_demographic)

    def test_campaign_creative_length(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        _, campaign = self._create_opportunity_campaign(
            "1", opp_data=dict(start=start_date, end=end_date))
        ad_group = AdGroup.objects.create(id="1", campaign=campaign)
        creative_duration_1 = 123
        creative_duration_2 = 2123
        creative_1 = VideoCreative.objects.create(
            id="1", duration=creative_duration_1)
        creative_2 = VideoCreative.objects.create(
            id="2", duration=creative_duration_2)
        VideoCreativeStatistic.objects.create(ad_group=ad_group,
                                              creative=creative_1,
                                              date=start_date)
        VideoCreativeStatistic.objects.create(ad_group=ad_group,
                                              creative=creative_2,
                                              date=start_date)
        response = self._request(start=str(start_date), end=str(end_date))

        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        self.assertEqual(set(campaign_data["creative_lengths"]),
                         {creative_duration_1, creative_duration_2})

    def test_campaign_thumbnail(self):
        _, campaign = self._create_opportunity_campaign("1")
        expected_video = "expected thumbnail"
        expected_thumbnail = "https://i.ytimg.com/vi/{}/hqdefault.jpg".format(
            expected_video)

        ad_group = AdGroup.objects.create(id="1", campaign=campaign)
        creative = VideoCreative.objects.create(id=expected_video)
        VideoCreativeStatistic.objects.create(ad_group=ad_group,
                                              creative=creative,
                                              cost=999,
                                              date=datetime(2017, 1, 1))
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        campaign_data = response.data["items"][0]["campaigns"][0]
        self.assertEqual(campaign_data["thumbnail"], expected_thumbnail)

    def test_compare_yoy_only(self):
        now = datetime(2017, 1, 3, 13, 0, 0)
        opportunity_1, _ = self._create_opportunity_campaign(
            "1", camp_data=dict(start_date=datetime(2015, 1, 1),
                                end_date=datetime(2015, 2, 1)))
        opportunity_2, _ = self._create_opportunity_campaign(
            "2", camp_data=dict(start_date=datetime(2016, 1, 1),
                                end_date=datetime(2016, 2, 1)))
        opportunity_3, _ = self._create_opportunity_campaign(
            "3", camp_data=dict(start_date=datetime(2017, 1, 1),
                                end_date=datetime(2017, 2, 1)))
        with patch_now(now):
            response = self._request(compare_yoy=True)

        opportunities = response.data["items"]
        self.assertEqual(len(opportunities), 2)
        actual_ids = {o["id"] for o in opportunities}
        self.assertEqual(actual_ids, {opportunity_2.id, opportunity_3.id})

    def test_sum_video_view_rate(self):
        opportunity = Opportunity.objects.create(id="1",
                                                 name="")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV)
        account = Account.objects.create(id="1", name="")

        stats = [
            (205893, 17471505),
            (327882, 31429643),
            (323852, 886421),
            (147386, 16631140),
            (44371, 187595),
            (167230, 640721),
            (111413, 389151),
            (36177, 138359),
            (13543, 48091),
            (11157, 1318436),
            (4432, 10482),
            (0, 0),
        ]
        for i, stat in enumerate(stats):
            video_views, impressions = stat
            campaign = Campaign.objects.create(id=str(i), account=account,
                                               salesforce_placement=placement,
                                               video_views=1,
                                               impressions=1)
            CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                             campaign=campaign,
                                             impressions=impressions,
                                             video_views=video_views)
        response = self._request(min_video_view_rate=50)

        self.assertEqual(len(response.data["items"]), 0)

    def test_filter_ctr_v_and_date(self):
        start, end = date(2017, 1, 1), date(2017, 2, 1)
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False,
            opp_data=dict(start=start, end=end))
        opportunity_2, campaign_2 = self._create_opportunity_campaign(
            "2", generate_statistic=False,
            opp_data=dict(start=start, end=end))
        CampaignStatistic.objects.create(date=start - timedelta(days=1),
                                         campaign=campaign_1,
                                         clicks=10000,
                                         video_views=1000000)  # 1%
        CampaignStatistic.objects.create(date=start,
                                         campaign=campaign_1,
                                         clicks=3,
                                         video_views=100)  # 3%

        CampaignStatistic.objects.create(date=start - timedelta(days=1),
                                         campaign=campaign_2,
                                         clicks=500000,
                                         video_views=1000000)
        CampaignStatistic.objects.create(date=start,
                                         campaign=campaign_2,
                                         clicks=29,
                                         video_views=1000)  # 3%
        response = self._request(start=str(start), end=str(end),
                                 min_ctr_v=3.)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity_1.id)

    def test_filter_ctr_and_date(self):
        start, end = date(2017, 1, 1), date(2017, 2, 1)
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False,
            opp_data=dict(start=start, end=end))
        opportunity_2, campaign_2 = self._create_opportunity_campaign(
            "2", generate_statistic=False,
            opp_data=dict(start=start, end=end))
        CampaignStatistic.objects.create(date=start - timedelta(days=1),
                                         campaign=campaign_1,
                                         clicks=10000,
                                         impressions=1000000)  # 1%
        CampaignStatistic.objects.create(date=start,
                                         campaign=campaign_1,
                                         clicks=3,
                                         impressions=100)  # 3%

        CampaignStatistic.objects.create(date=start - timedelta(days=1),
                                         campaign=campaign_2,
                                         clicks=500000,
                                         impressions=1000000)
        CampaignStatistic.objects.create(date=start,
                                         campaign=campaign_2,
                                         clicks=29,
                                         impressions=1000)  # 3%
        response = self._request(start=str(start), end=str(end),
                                 min_ctr=3.)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity_1.id)

    def test_periods_filter_first(self):
        expected_opportunities_count = 3
        expected_ids = set()
        opportunities_periods = (
            (datetime(2017, 12, 4), datetime(2017, 12, 30), False),
            (datetime(2017, 12, 4), datetime(2018, 1, 24), True),
            (datetime(2018, 1, 4), datetime(2018, 1, 28), True),
            (datetime(2018, 3, 4), datetime(2018, 4, 11), True),
            (datetime(2018, 4, 1), datetime(2018, 6, 11), False),
        )
        for i, date_range in enumerate(opportunities_periods):
            _id = str(1 + i)
            camp_data = {
                "start_date": date_range[0],
                "end_date": date_range[1]
            }
            opportunity, _ = self._create_opportunity_campaign(
                _id=_id, camp_data=camp_data)
            if date_range[2]:
                expected_ids.add(opportunity.id)
        response = self._request(start="2018-01-01", end="2018-03-31")
        self.assertEqual(
            response.data.get("items_count"), expected_opportunities_count)
        self.assertEqual(
            {obj.get("id") for obj in response.data.get("items")},
            expected_ids)

    def test_periods_filter_second(self):
        expected_opportunities_count = 6
        expected_ids = set()
        now = datetime(2018, 12, 31)
        opportunities_periods = (
            (datetime(2017, 12, 4), datetime(2017, 12, 30), False),
            (datetime(2017, 12, 4), datetime(2018, 1, 24), True),
            (datetime(2018, 1, 4), datetime(2018, 1, 28), True),
            (datetime(2018, 3, 4), datetime(2018, 4, 11), True),
            (datetime(2018, 4, 1), datetime(2018, 6, 11), False),
            (datetime(2018, 6, 11), datetime(2018, 7, 11), True),
            (datetime(2018, 8, 13), datetime(2018, 8, 23), True),
            (datetime(2018, 9, 1), datetime(2018, 12, 14), True),
            (datetime(2018, 10, 1), datetime(2018, 12, 14), False),
        )
        for i, date_range in enumerate(opportunities_periods):
            _id = str(1 + i)
            camp_data = {
                "start_date": date_range[0],
                "end_date": date_range[1]
            }
            opportunity, _ = self._create_opportunity_campaign(
                _id=_id, camp_data=camp_data)
            if date_range[2]:
                expected_ids.add(opportunity.id)
        with patch_now(now):
            response = self._request(quarters=["Q1", "Q3"])
        self.assertEqual(
            response.data.get("items_count"), expected_opportunities_count)
        self.assertEqual(
            {obj.get("id") for obj in response.data.get("items")},
            expected_ids)

    def test_opportunity_budget_for_period(self):
        start = end = date(2017, 1, 1)
        opportunity, campaign = self._create_opportunity_campaign(
            "1", opp_data=dict(start=start, end=end), generate_statistic=False)
        expected_cost = 1234
        extra_cost = 4421
        CampaignStatistic.objects.create(date=start,
                                         campaign=campaign,
                                         cost=expected_cost)
        CampaignStatistic.objects.create(date=start - timedelta(days=1),
                                         campaign=campaign,
                                         cost=extra_cost)
        CampaignStatistic.objects.create(date=start + timedelta(days=1),
                                         campaign=campaign,
                                         cost=extra_cost)
        response = self._request(start=str(start), end=str(end))

        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["budget"], expected_cost)

    def test_sf_cpm(self):
        opportunity = Opportunity.objects.create()
        total_costs = (123, 234)
        ordered_units = (432, 123)
        expected_cpm = sum(total_costs) / sum(ordered_units) * 1000
        _id = 1
        for cost, units in zip(total_costs, ordered_units):
            placement = OpPlacement.objects.create(
                id=_id,
                opportunity=opportunity,
                goal_type_id=SalesForceGoalType.CPM,
                total_cost=cost, ordered_units=units)
            Campaign.objects.create(id=_id, salesforce_placement=placement)
            _id += 1
        placement = OpPlacement.objects.create(
            id=_id,
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            total_cost=444, ordered_units=555)
        Campaign.objects.create(id=_id, salesforce_placement=placement)
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["sf_cpm"], expected_cpm)

    def test_sf_cpm_filtered_by_date(self):
        opportunity = Opportunity.objects.create()
        costs = (123, 234)
        units = (432, 124)
        start, end = date(2017, 1, 1), date(2017, 4, 1)
        expected_cpm = sum(costs) / sum(units) * 1000

        data = (
            (start - timedelta(days=1), end - timedelta(days=1),
             costs[0], units[0]),
            (start + timedelta(days=1), end + timedelta(days=1),
             costs[1], units[1]),
            (start - timedelta(days=2), start - timedelta(days=1),
             444, 555),
            (end + timedelta(days=1), end + timedelta(days=2),
             666, 777),
        )
        for i, d in enumerate(data):
            p_start, p_end, cost, ordered_units = d
            placement = OpPlacement.objects.create(
                id=i,
                opportunity=opportunity,
                goal_type_id=SalesForceGoalType.CPM,
                start=p_start, end=p_end,
                total_cost=cost, ordered_units=ordered_units)
            Campaign.objects.create(id=i, salesforce_placement=placement)
        response = self._request(start=str(start), end=str(end))

        self.assertEqual(len(response.data["items"]), 1)
        self.assertAlmostEqual(response.data["items"][0]["sf_cpm"],
                               expected_cpm)

    def test_sf_cpv(self):
        opportunity = Opportunity.objects.create()
        total_costs = (123, 234)
        ordered_units = (432, 123)
        expected_cpm = sum(total_costs) / sum(ordered_units)
        _id = 1
        for cost, units in zip(total_costs, ordered_units):
            placement = OpPlacement.objects.create(
                id=_id,
                opportunity=opportunity,
                goal_type_id=SalesForceGoalType.CPV,
                total_cost=cost, ordered_units=units)
            Campaign.objects.create(id=_id, salesforce_placement=placement)
            _id += 1
        placement = OpPlacement.objects.create(
            id=_id,
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            total_cost=444, ordered_units=555)
        Campaign.objects.create(id=_id, salesforce_placement=placement)
        response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["sf_cpv"], expected_cpm)

    def test_sf_cpv_filtered_by_date(self):
        opportunity = Opportunity.objects.create()
        costs = (123, 234)
        units = (432, 123)
        start, end = date(2017, 1, 1), date(2017, 4, 1)
        request_start, request_end = str(start), str(end)
        expected_cpm = sum(costs) / sum(units)

        data = (
            (start - timedelta(days=1), end - timedelta(days=1),
             costs[0], units[0]),
            (start + timedelta(days=1), end + timedelta(days=1),
             costs[1], units[1]),
            (start - timedelta(days=2), start - timedelta(days=1),
             444, 555),
            (end + timedelta(days=1), end + timedelta(days=2),
             444, 555),
        )
        for i, d in enumerate(data):
            start, end, cost, ordered_units = d
            placement = OpPlacement.objects.create(
                id=i,
                opportunity=opportunity,
                goal_type_id=SalesForceGoalType.CPV,
                start=start, end=end,
                total_cost=cost, ordered_units=ordered_units)
            Campaign.objects.create(id=i, salesforce_placement=placement)
        response = self._request(start=request_start, end=request_end)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["sf_cpv"], expected_cpm)

    def test_average_cpv_filtered_by_date(self):
        start_end = date(2017, 1, 1)
        _, campaign = self._create_opportunity_campaign(
            "1", SalesForceGoalType.CPV, generate_statistic=False,
            opp_data=dict(start=start_end, end=start_end))
        video_views, cost = 100, 5
        average_cpv = cost / video_views
        CampaignStatistic.objects.create(campaign=campaign, date=start_end,
                                         video_views=video_views, cost=cost)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=start_end - timedelta(days=1),
                                         video_views=9999, cost=9999)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=start_end + timedelta(days=1),
                                         video_views=9999, cost=9999)
        response = self._request(start=str(start_end), end=str(start_end))

        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["average_cpv"], average_cpv)

    def test_average_cpm_filtered_by_date(self):
        start_end = date(2017, 1, 1)
        _, campaign = self._create_opportunity_campaign(
            "1", SalesForceGoalType.CPM, generate_statistic=False,
            opp_data=dict(start=start_end, end=start_end))
        impressions, cost = 100, 5
        average_cpm = cost / impressions * 1000
        CampaignStatistic.objects.create(campaign=campaign, date=start_end,
                                         impressions=impressions, cost=cost)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=start_end - timedelta(days=1),
                                         impressions=9999, cost=9999)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=start_end + timedelta(days=1),
                                         impressions=9999, cost=9999)
        response = self._request(start=str(start_end), end=str(start_end))

        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["average_cpm"], average_cpm)

    def test_margin_with_date(self):
        start_end = date(2017, 1, 1)
        opportunity = Opportunity.objects.create(id="1",
                                                 start=start_end, end=start_end)

        cpm_placement = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            start=start_end, end=start_end,
            ordered_rate=12., total_cost=99999
        )
        cpv_placement = OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            start=start_end, end=start_end,
            ordered_rate=0.5, total_cost=99999
        )

        cpm_campaign = Campaign.objects.create(
            id="1", salesforce_placement=cpm_placement)
        cpv_campaign = Campaign.objects.create(
            id="2", salesforce_placement=cpv_placement)
        cpm_cost, cpv_cost = 245, 543
        cpm_impressions, cpv_views = 4567, 432

        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=cpm_campaign,
                                         impressions=cpm_impressions,
                                         video_views=999999,
                                         cost=cpm_cost)
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=cpv_campaign,
                                         impressions=999999,
                                         video_views=cpv_views,
                                         cost=cpv_cost)

        def generate_extra_statistic(cp, dt):
            CampaignStatistic.objects.create(date=dt,
                                             campaign=cp,
                                             impressions=999999,
                                             video_views=999999,
                                             cost=999999)

        generate_extra_statistic(cpm_campaign, start_end - timedelta(days=1))
        generate_extra_statistic(cpv_campaign, start_end - timedelta(days=1))
        generate_extra_statistic(cpm_campaign, start_end + timedelta(days=1))
        generate_extra_statistic(cpv_campaign, start_end + timedelta(days=1))

        sf_cpm = cpm_placement.ordered_rate / 1000
        sf_cpv = cpv_placement.ordered_rate
        expected_margin = (1 - (cpm_cost + cpv_cost) / (
                cpm_impressions * sf_cpm + cpv_views * sf_cpv)) * 100
        response = self._request(start=str(start_end), end=str(start_end))

        self.assertAlmostEqual(response.data["items"][0]["margin"],
                               expected_margin)

    def test_filter_opportunity_from_hidden_accounts(self):
        _, campaign = self._create_opportunity_campaign("1")
        campaign.account = Account.objects.create(id="1")
        campaign.save()
        user_settings = {UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
                         UserSettingsKey.VISIBLE_ACCOUNTS: []}
        with self.patch_user_settings(**user_settings):
            response = self._request()

        self.assertEqual(len(response.data["items"]), 0)

    def test_shows_campaign_without_account(self):
        opportunity = Opportunity.objects.create(id="1")
        placement = OpPlacement.objects.create(id="1", opportunity=opportunity)
        campaign = Campaign.objects.create(
            id="1", salesforce_placement=placement, account=None)
        with self.patch_user_settings(visible_accounts=[]):
            response = self._request()

        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)
        self.assertEqual(len(response.data["items"][0]["campaigns"]), 1)
        self.assertEqual(response.data["items"][0]["campaigns"][0]["id"],
                         campaign.id)

    def test_filter_hidden_campaigns(self):
        _, campaign_1 = self._create_opportunity_campaign("1")
        account_1 = Account.objects.create(id="1")
        campaign_1.account = account_1
        campaign_1.save()
        placement = campaign_1.salesforce_placement
        account_2 = Account.objects.create(id="2", name="")
        Campaign.objects.create(
            id="2", account=account_2, salesforce_placement=placement)
        user_settings = {UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
                         UserSettingsKey.VISIBLE_ACCOUNTS: [account_1.id]}
        with self.patch_user_settings(**user_settings):
            response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        campaign_ids = [c["id"] for c in response.data["items"][0]["campaigns"]]
        self.assertEqual(campaign_ids, [campaign_1.id])

    def test_filter_by_video100rate_ignores_hidden_accounts(self):
        _, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False)
        account_1 = Account.objects.create(id="1")
        campaign_1.account = account_1
        campaign_1.save()
        placement = campaign_1.salesforce_placement
        account_2 = Account.objects.create(id="2", name="")
        campaign_2 = Campaign.objects.create(
            id="2", account=account_2, salesforce_placement=placement,
            video_views_100_quartile=20, impressions=100)
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign_1,
                                         video_views_100_quartile=1,
                                         impressions=100)
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign_2,
                                         video_views_100_quartile=20,
                                         impressions=100)

        user_settings = {UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
                         UserSettingsKey.VISIBLE_ACCOUNTS: [account_1.id]}
        with self.patch_user_settings(**user_settings):
            response = self._request(min_video100rate=10,
                                     max_video100rate=30)

        self.assertEqual(len(response.data["items"]), 0)

    def test_filter_by_video_rate_ignores_hidden_accounts(self):
        _, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False)
        account_1 = Account.objects.create(id="1")
        campaign_1.account = account_1
        campaign_1.save()
        placement = campaign_1.salesforce_placement
        account_2 = Account.objects.create(id="2", name="")
        campaign_2 = Campaign.objects.create(
            id="2", account=account_2, salesforce_placement=placement,
            video_views_100_quartile=20, impressions=100)
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign_2,
                                         video_views=2,
                                         impressions=10)
        user_settings = {UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
                         UserSettingsKey.VISIBLE_ACCOUNTS: [account_1.id]}
        with self.patch_user_settings(**user_settings):
            response = self._request(min_video_view_rate=10,
                                     max_video_view_rate=30)

        self.assertEqual(len(response.data["items"]), 0)

    def test_opportunity_and_campaigns_metrics_values(self):
        today = timezone.now().date()
        period_days = 10
        start, end = today - timedelta(days=period_days), today
        opportunity = Opportunity.objects.create(
            id="opportunity_1", name="", brand="Test")
        placement_1 = OpPlacement.objects.create(
            id="op_placement_1", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV, ordered_rate=0.6)
        placement_2 = OpPlacement.objects.create(
            id="op_placement_2", name="", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM, ordered_rate=0.6)
        predefined_cpv_statistics = {
            "impressions": 112,
            "video_views": 143,
            "clicks": 78}
        campaign = Campaign.objects.create(
            id="campaign_1", name="Campaign name 1",
            salesforce_placement=placement_1,
            **predefined_cpv_statistics)
        generate_campaign_statistic(
            campaign, start, end, predefined_cpv_statistics)
        predefined_cpm_statistics = {
            "impressions": 26,
            "video_views": 0,
            "clicks": 11}
        campaign = Campaign.objects.create(
            id="campaign_2", name="Campaign name 2",
            salesforce_placement=placement_2,
            **predefined_cpm_statistics)
        generate_campaign_statistic(
            campaign, start, end, predefined_cpm_statistics)
        expected_campaigns_ctr = set()
        expected_campaigns_ctr_v = set()
        for stats in [predefined_cpv_statistics, predefined_cpm_statistics]:
            ctr = (stats["clicks"] / stats["video_views"]) * 100 \
                if stats["video_views"] else 0
            expected_campaigns_ctr.add(ctr)
            expected_campaigns_ctr_v.add(
                stats["clicks"] / stats["impressions"] * 100)
        expected_opportunity_ctr_v = sum(
            (predefined_cpv_statistics["clicks"],
             predefined_cpm_statistics["clicks"])) / \
                                     sum((predefined_cpv_statistics[
                                              "video_views"],
                                          predefined_cpm_statistics[
                                              "video_views"])) * 100
        expected_opportunity_ctr = sum(
            (predefined_cpv_statistics["clicks"],
             predefined_cpm_statistics["clicks"])) / \
                                   sum((
                                       predefined_cpv_statistics["impressions"],
                                       predefined_cpm_statistics[
                                           "impressions"])) * 100
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        opportunity = response.data["items"][0]
        campaigns = opportunity["campaigns"]
        self.assertEqual({c["ctr"] for c in campaigns}, expected_campaigns_ctr)
        self.assertEqual(
            {c["ctr_v"] for c in campaigns}, expected_campaigns_ctr_v)
        self.assertEqual(opportunity["ctr"], expected_opportunity_ctr)
        self.assertEqual(opportunity["ctr_v"], expected_opportunity_ctr_v)

    def test_opportunity_and_campaigns_metrics_values_view_rate(self):
        opportunity = Opportunity.objects.create(id="1")
        placement_1 = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV)
        placement_2 = OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM)
        campaign_1 = Campaign.objects.create(id="1",
                                             salesforce_placement=placement_1)
        campaign_2 = Campaign.objects.create(id="2",
                                             salesforce_placement=placement_1)
        campaign_3 = Campaign.objects.create(id="3",
                                             salesforce_placement=placement_2)

        video_views = 124, 432
        impressions = 2345, 4123
        date_filter = date(2017, 1, 1)

        # include
        CampaignStatistic.objects.create(date=date_filter, campaign=campaign_1,
                                         video_views=video_views[0],
                                         impressions=impressions[0])
        CampaignStatistic.objects.create(date=date_filter, campaign=campaign_2,
                                         video_views=video_views[1],
                                         impressions=impressions[1])

        # exclude: not CPV
        CampaignStatistic.objects.create(date=date_filter, campaign=campaign_3,
                                         video_views=999999,
                                         impressions=999999)
        # exclude: out of date
        CampaignStatistic.objects.create(date=date_filter + timedelta(days=1),
                                         campaign=campaign_1,
                                         video_views=999999,
                                         impressions=999999)
        expected_opportunity_view_rate = sum(video_views) * 100. \
                                         / sum(impressions)
        response = self._request(start=str(date_filter), end=str(date_filter))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        opportunity_data = response.data["items"][0]
        self.assertAlmostEqual(opportunity_data["view_rate"],
                               expected_opportunity_view_rate)

        campaigns = response.data["items"][0]["campaigns"]
        self.assertEqual(len(campaigns), 3)

    def test_opportunity_not_conflict_aggregation(self):
        total_costs = (1243, 5432)
        ordered_units = (54325, 123)
        opportunity, campaign = self._create_opportunity_campaign(
            "1", generate_statistic=False,
            pl_data=dict(ordered_units=ordered_units[0],
                         total_cost=total_costs[0]))
        AdGroup.objects.create(id="1", campaign=campaign, type="12")
        AdGroup.objects.create(id="2", campaign=campaign, type="123")

        OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            total_cost=total_costs[1],
            ordered_units=ordered_units[1]
        )

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        opp_data = response.data["items"][0]
        self.assertEqual(opp_data["sf_cpv"],
                         sum(total_costs) / sum(ordered_units))

    def test_campaign_video100rate(self):
        date_filter = date(2017, 1, 1)
        _, campaign = self._create_opportunity_campaign(
            "1",
            opp_data=dict(start=date_filter, end=date_filter),
            generate_statistic=False)
        video_views_100_quartile = 45
        impressions = 100
        CampaignStatistic.objects.create(
            date=date_filter,
            campaign=campaign,
            video_views_100_quartile=video_views_100_quartile,
            impressions=impressions)
        CampaignStatistic.objects.create(
            date=date_filter + timedelta(days=1),
            campaign=campaign,
            video_views_100_quartile=1000,
            impressions=1000)
        expected_video100rate = video_views_100_quartile * 100. / impressions
        response = self._request(start=str(date_filter), end=str(date_filter))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        self.assertEqual(len(response.data["items"][0]["campaigns"]), 1)
        opportunity_data = response.data["items"][0]
        self.assertEqual(opportunity_data["video100rate"],
                         expected_video100rate)

    def test_filter_by_ctr_with_zero_impressions(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False)
        opportunity_2, campaign_2 = self._create_opportunity_campaign(
            "2", generate_statistic=False)

        CampaignStatistic.objects.create(campaign=campaign_1,
                                         date=date(2017, 1, 1),
                                         clicks=20,
                                         impressions=0)
        CampaignStatistic.objects.create(campaign=campaign_2,
                                         date=date(2017, 1, 1),
                                         clicks=40,
                                         impressions=1000)
        response = self._request(min_ctr=0, max_ctr=0)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data["items"]), 1)
        self.assertEqual(data["items"][0]["id"], opportunity_1.id)

    def test_filter_by_ctr_v_with_zero_video_views(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False)
        opportunity_2, campaign_2 = self._create_opportunity_campaign(
            "2", generate_statistic=False)

        CampaignStatistic.objects.create(campaign=campaign_1,
                                         date=date(2017, 1, 1),
                                         clicks=20,
                                         video_views=0)
        CampaignStatistic.objects.create(campaign=campaign_2,
                                         date=date(2017, 1, 1),
                                         clicks=40,
                                         video_views=1000)
        response = self._request(max_ctr_v=0, min_ctr_v=0)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data["items"]), 1)
        self.assertEqual(data["items"][0]["id"], opportunity_1.id)

    def test_filter_by_view_rate_with_zero_video_views(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False)
        _, campaign_2 = self._create_opportunity_campaign(
            "2", generate_statistic=False)
        CampaignStatistic.objects.create(campaign=campaign_1,
                                         date=date(2017, 1, 1),
                                         video_views=200,
                                         impressions=0)
        CampaignStatistic.objects.create(campaign=campaign_2,
                                         date=date(2017, 1, 1),
                                         video_views=400,
                                         impressions=1000)
        response = self._request(min_video_view_rate=0,
                                 max_video_view_rate=0)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data["items"]), 1)
        self.assertEqual(data["items"][0]["id"], opportunity_1.id)

    def test_filter_by_video100rate_with_zero_video_views(self):
        opportunity_1, campaign_1 = self._create_opportunity_campaign(
            "1", generate_statistic=False)
        _, campaign_2 = self._create_opportunity_campaign(
            "2", generate_statistic=False)

        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign_1,
                                         video_views_100_quartile=20,
                                         impressions=0)
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=campaign_2,
                                         video_views_100_quartile=40,
                                         impressions=100)
        response = self._request(min_video100rate=0,
                                 max_video100rate=0)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(len(data["items"]), 1)
        self.assertEqual(data["items"][0]["id"], opportunity_1.id)

    def test_margin_hard_cost_zero_our_cost(self):
        _, campaign = self._create_opportunity_campaign(
            "1", SalesForceGoalType.HARD_COST, generate_statistic=False)
        placement = campaign.salesforce_placement
        Flight.objects.create(placement=placement,
                              total_cost=1)
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["margin"], 100)

    def test_margin_hard_cost_zero_client_cost(self):
        _, campaign = self._create_opportunity_campaign(
            "1", SalesForceGoalType.HARD_COST)
        Flight.objects.create(placement=campaign.salesforce_placement,
                              cost=1)
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["margin"], -100)

    def test_margin_hard_cost_and_cpv_zero_client_cost(self):
        ordered_cpv = .7
        aw_cpv = .3
        delivered_units = 1000
        cpv_client_cost = delivered_units * ordered_cpv
        aw_cpv_cost = delivered_units * aw_cpv
        hard_cost_cost = 20
        hard_cost_client_cost = 200
        client_cost = hard_cost_client_cost + cpv_client_cost
        cost = hard_cost_cost + aw_cpv_cost
        expected_margin = (client_cost - cost) / client_cost * 100
        opportunity, hard_cost_campaign = self._create_opportunity_campaign(
            "1", SalesForceGoalType.HARD_COST,
            pl_data=dict(total_cost=9999),
            generate_statistic=False)
        Flight.objects.create(total_cost=hard_cost_client_cost,
                              cost=hard_cost_cost,
                              placement=hard_cost_campaign.salesforce_placement)
        cpv_placement = OpPlacement.objects.create(
            id="2", goal_type_id=SalesForceGoalType.CPV,
            opportunity=opportunity,
            total_cost=9999,
            ordered_rate=ordered_cpv)
        cpv_campaign = Campaign.objects.create(
            id="2", salesforce_placement=cpv_placement)
        CampaignStatistic.objects.create(date=date(2017, 1, 1),
                                         campaign=cpv_campaign,
                                         video_views=delivered_units,
                                         cost=aw_cpv_cost)
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 1)
        self.assertAlmostEqual(items[0]["margin"], expected_margin)

    def test_hard_cost(self):
        client_cost = 400
        cost = 300
        expected_margin = (client_cost - cost) * 1. / client_cost * 100
        opportunity = Opportunity.objects.create(probability=50)
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            total_cost=999)
        Campaign.objects.create(salesforce_placement=placement)
        Flight.objects.create(placement=placement,
                              cost=cost,
                              total_cost=client_cost)
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 1)
        self.assertAlmostEqual(items[0]["margin"], expected_margin)

    def test_hard_cost_filtered_by_date(self):
        today = date(2017, 1, 1)
        client_cost = 400
        cost = 300
        expected_margin = (client_cost - cost) * 1. / client_cost * 100
        opportunity = Opportunity.objects.create(probability=50)
        placement = OpPlacement.objects.create(
            opportunity=opportunity,
            goal_type_id=SalesForceGoalType.HARD_COST,
            total_cost=9999999)
        Campaign.objects.create(salesforce_placement=placement)
        Flight.objects.create(id="1",
                              start=today - timedelta(days=10),
                              end=today + timedelta(days=2),
                              placement=placement,
                              cost=cost,
                              total_cost=client_cost)
        Flight.objects.create(id="2",
                              start=today + timedelta(days=1),
                              end=today + timedelta(days=10),
                              placement=placement,
                              cost=999999,
                              total_cost=999999)
        response = self._request(start=str(today),
                                 end=str(today))

        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 1)
        self.assertAlmostEqual(items[0]["margin"], expected_margin)

    def test_hard_cost_margin(self):
        opportunity = Opportunity.objects.create(probability=50)
        placement_cpm = OpPlacement.objects.create(
            id="1",
            opportunity=opportunity,
            ordered_rate=0.2,
            total_cost=9999,
            goal_type_id=SalesForceGoalType.CPM)
        placement_cpv = OpPlacement.objects.create(
            id="2",
            opportunity=opportunity,
            ordered_rate=0.3,
            total_cost=9999,
            goal_type_id=SalesForceGoalType.CPV)
        placement_hard_cost = OpPlacement.objects.create(
            id="3",
            opportunity=opportunity,
            total_cost=9999,
            goal_type_id=SalesForceGoalType.HARD_COST)
        Flight.objects.create(id="1", placement=placement_cpm,
                              total_cost=9999)
        Flight.objects.create(id="2", placement=placement_cpv,
                              total_cost=9999)
        flight_hard_cost = Flight.objects.create(id="3",
                                                 placement=placement_hard_cost,
                                                 cost=324,
                                                 total_cost=9999)
        campaign_cpm = Campaign.objects.create(
            id="1",
            salesforce_placement=placement_cpm
        )
        campaign_cpv = Campaign.objects.create(
            id="2",
            salesforce_placement=placement_cpv
        )
        campaign_hard_cost = Campaign.objects.create(
            id="3",
            salesforce_placement=placement_hard_cost
        )

        today = date(2017, 1, 1)
        impressions = 23499
        views = 654
        cpm_cost, cpv_cost = 543, 356
        CampaignStatistic.objects.create(date=today, campaign=campaign_cpm,
                                         impressions=impressions, cost=cpm_cost)
        CampaignStatistic.objects.create(date=today, campaign=campaign_cpv,
                                         video_views=views, cost=cpv_cost)
        CampaignStatistic.objects.create(date=today,
                                         campaign=campaign_hard_cost)

        client_cost = impressions * placement_cpm.ordered_rate / 1000. \
                      + views * placement_cpv.ordered_rate \
                      + flight_hard_cost.total_cost
        cost = cpm_cost + cpv_cost + flight_hard_cost.cost

        margin = (1 - cost / client_cost) * 100.

        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        opp_data = response.data["items"][0]
        self.assertAlmostEqual(opp_data["margin"], margin)

    def test_hard_cost_campaign_flight_filtering(self):
        opportunity = Opportunity.objects.create(id=1, probability=50)
        today = date(2017, 1, 15)
        start_1, end_1 = date(2017, 1, 1), date(2017, 1, 31)
        start_2, end_2 = date(2017, 2, 1), date(2017, 2, 28)
        aw_cost = 234
        impressions = 22222222

        placement_cpm = OpPlacement.objects.create(
            id="1",
            start=min(start_1, start_2),
            end=max(end_1, end_2),
            opportunity=opportunity,
            ordered_rate=0.2,
            total_cost=9999,
            goal_type_id=SalesForceGoalType.CPM)
        placement_hc = OpPlacement.objects.create(
            id="2",
            start=min(start_1, start_2),
            end=max(end_1, end_2),
            opportunity=opportunity,
            total_cost=9999,
            goal_type_id=SalesForceGoalType.HARD_COST)
        flight_hard_cost_1 = Flight.objects.create(id="1",
                                                   placement=placement_hc,
                                                   start=start_1,
                                                   end=end_1,
                                                   cost=3240,
                                                   total_cost=9999)
        flight_hard_cost_2 = Flight.objects.create(id="2",
                                                   placement=placement_hc,
                                                   start=start_2,
                                                   end=end_2,
                                                   cost=3240,
                                                   total_cost=9999)
        campaign = Campaign.objects.create(salesforce_placement=placement_cpm)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=date(2017, 1, 1),
                                         cost=aw_cost,
                                         impressions=impressions)
        cost = aw_cost + flight_hard_cost_1.cost
        client_cost = flight_hard_cost_1.total_cost \
                      + placement_cpm.ordered_rate * impressions / 1000.
        expected_margin = (1 - cost / client_cost) * 100

        with patch_now(today):
            response = self._request(quarters=["Q1"])

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        opp_data = response.data["items"][0]
        self.assertAlmostEqual(opp_data["margin"], expected_margin)

        cost = aw_cost + flight_hard_cost_1.cost + flight_hard_cost_2.cost
        client_cost = flight_hard_cost_1.total_cost + \
                      flight_hard_cost_2.total_cost \
                      + placement_cpm.ordered_rate * impressions / 1000.
        expected_margin = (1 - cost / client_cost) * 100

        with patch_now(end_2):
            response = self._request(quarters=["Q1"])

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 1)
        opp_data = response.data["items"][0]
        self.assertAlmostEqual(opp_data["margin"], expected_margin)

    def test_filtering_targeting_and(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/SAAS-2418
        Summary: Pricing tool > Targeting Type filter incorrectly works
        """
        opportunity = Opportunity.objects.create(id=1)
        opportunity.refresh_from_db()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        Campaign.objects.create(id=1, salesforce_placement=placement,
                                has_channels=True)
        Campaign.objects.create(id=2, salesforce_placement=placement,
                                has_interests=True)
        response = self._request(targeting_types=["interests", "channels"],
                                 targeting_types_condition="and")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_filtering_by_interests(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/SAAS-2426
        Summary: Some of the opportunities absent in result set after filtration
              by Interests,  despite of the fact they include selected interests

        Root cause:
        SQL calculates count of related interests (regarding hierarchy)
        by each group and set `top_count` by &(count1, count2, ...),
        then filters opportunities with `top_count` > 0.
            ! It's wrong because &(1, 2) == 0
        """
        opportunity = Opportunity.objects.create(id=1)
        opportunity.refresh_from_db()
        placement = OpPlacement.objects.create(id=1, opportunity=opportunity)
        campaign = Campaign.objects.create(id=1, salesforce_placement=placement)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign)
        a_1 = Audience.objects.create(id=1, name="/A 1")
        a_2 = Audience.objects.create(id=2, name="/A 2")
        a_2_1 = Audience.objects.create(id=21, name="/A 2/1", parent=a_2)
        statistic_common = dict(date=date(2017, 1, 1), ad_group=ad_group)
        AudienceStatistic.objects.create(audience=a_1, **statistic_common)
        AudienceStatistic.objects.create(audience=a_2, **statistic_common)
        AudienceStatistic.objects.create(audience=a_2_1, **statistic_common)

        response = self._request(
            interests=[a_1.id, a_2.id],
            interests_condition=Operator.AND
        )

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_filtering_by_topics(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/SAAS-2426
        Summary: Some of the opportunities absent in result set after filtration
              by Interests,  despite of the fact they include selected interests

        Root cause:
        SQL calculates count of related topics (regarding hierarchy)
        by each group and set `top_count` by &(count1, count2, ...),
        then filters opportunities with `top_count` > 0.
            ! It's wrong because &(1, 2) == 0
        """
        opportunity = Opportunity.objects.create(id=1)
        opportunity.refresh_from_db()
        placement = OpPlacement.objects.create(id=1, opportunity=opportunity)
        campaign = Campaign.objects.create(id=1, salesforce_placement=placement)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign)
        t_1 = Topic.objects.create(id=1, name="/A 1")
        t_2 = Topic.objects.create(id=2, name="/A 2")
        t_2_1 = Topic.objects.create(id=21, name="/A 2/1", parent=t_2)
        statistic_common = dict(date=date(2017, 1, 1), ad_group=ad_group)
        TopicStatistic.objects.create(topic=t_1, **statistic_common)
        TopicStatistic.objects.create(topic=t_2, **statistic_common)
        TopicStatistic.objects.create(topic=t_2_1, **statistic_common)

        response = self._request(
            topics=[t_1.id, t_2.id],
            topics_condition=Operator.AND
        )

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["id"], opportunity.id)

    def test_margin_on_campaign_level(self):
        start_end = date(2017, 1, 1)
        opportunity = Opportunity.objects.create(id="1",
                                                 start=start_end, end=start_end)

        cpm_placement = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM,
            start=start_end, end=start_end,
            ordered_rate=12., total_cost=99999
        )
        cpv_placement = OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV,
            start=start_end, end=start_end,
            ordered_rate=0.5, total_cost=99999
        )

        cpm_cost, cpv_cost = 245, 543
        cpm_impressions, cpv_views = 4567, 432
        cpm_campaign = Campaign.objects.create(
            id="1", salesforce_placement=cpm_placement,
            impressions=cpm_impressions,
            video_views=999999,
            cost=cpm_cost
        )
        cpv_campaign = Campaign.objects.create(
            id="2", salesforce_placement=cpv_placement,
            impressions=999999,
            video_views=cpv_views,
            cost=cpv_cost
        )

        sf_cpm = cpm_placement.ordered_rate / 1000
        sf_cpv = cpv_placement.ordered_rate

        cpm_margin = (1 - cpm_cost / (cpm_impressions * sf_cpm)) * 100
        cpv_margin = (1 - cpv_cost / (cpv_views * sf_cpv)) * 100

        response = self._request(start=str(start_end), end=str(start_end))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        margin_by_campaign = {c["id"]: c["margin"]
                              for c in response.data["items"][0]["campaigns"]}
        self.assertAlmostEqual(margin_by_campaign[cpm_campaign.id], cpm_margin)
        self.assertAlmostEqual(margin_by_campaign[cpv_campaign.id], cpv_margin)

    def test_margin_on_over_delivery(self):
        """
        Bug: because of wrong JOIN total_cost calculates
            as total_cost * flights count * campaign.
            So if there is more then one flight margin may be be wrong
        """
        budget = 5000
        views = 100000
        cost = 2000
        any_date = date(2018, 1, 1)
        client_cost = budget
        expected_margin = (1 - cost / client_cost) * 100
        opportunity = Opportunity.objects.create(budget=budget)
        placement = OpPlacement.objects.create(
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV,
            ordered_rate=6, total_cost=opportunity.budget)
        for i in range(2):
            Flight.objects.create(id=i, placement=placement)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        for i in range(1, 2):
            Campaign.objects.create(id=i, salesforce_placement=placement)
        CampaignStatistic.objects.create(date=any_date,
                                         campaign=campaign,
                                         video_views=views,
                                         cost=cost)
        # assert bug conditions
        self.assertGreater(placement.flights.count(), 1)
        self.assertGreater(placement.adwords_campaigns.count(), 1)
        self.assertGreater(views * placement.ordered_rate, budget)

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        opp_data = response.data["items"][0]
        self.assertAlmostEqual(opp_data["margin"], expected_margin)

    def test_empty_product(self):
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        AdGroup.objects.create(id=1, campaign=campaign, type="")
        test_type = "test-type"
        AdGroup.objects.create(id=2, campaign=campaign, type=test_type)

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(response.data["items"][0]["products"], [test_type])

    def test_aggregates_latest_data(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/VIQ-760
        Summary: Pricing Tool > Margin doesn't match with Margin on Pacing report (which is correct)
        Root cause 1: Different calculation of client cost for Hard Cost
        Root cause 2: Pacing report uses statistic filtered by flight dates, pricing tool does not.
        """
        test_now = datetime(2018, 1, 1, 14, 45, tzinfo=pytz.utc)
        start = (test_now - timedelta(days=5)).date()
        end = (test_now + timedelta(days=2)).date()
        ordered_units = 1234
        delivered_units = 345
        cost = 123

        opportunity = Opportunity.objects.create(probability=100)
        placement = OpPlacement.objects.create(id=next(int_iterator),
                                               opportunity=opportunity, goal_type_id=SalesForceGoalType.CPM,
                                               ordered_rate=12.2, total_cost=9999)

        Flight.objects.create(id=next(int_iterator),
                              placement=placement, start=start, end=end,
                              ordered_units=ordered_units, total_cost=9999)

        account = Account.objects.create()
        campaign = Campaign.objects.create(account=account, salesforce_placement=placement)
        CampaignStatistic.objects.create(date=test_now, campaign=campaign, impressions=delivered_units, cost=cost)

        client_cost = delivered_units * placement.ordered_rate / 1000
        expected_margin = (1 - cost / client_cost) * 100

        with patch_now(test_now):
            response = self._request()
        opportunities = response.data["items"]
        self.assertEqual(len(opportunities), 1)

        opp_data = opportunities[0]
        self.assertAlmostEqual(opp_data["margin"], expected_margin)
        self.fail("Test not covers the issue yet.")


def generate_campaign_statistic(
        campaign, start, end, predefined_statistics=None):
    for i in range((end - start).days + 1):
        base_stats = {
            "campaign": campaign,
            "date": start + timedelta(days=i),
            "impressions": 10,
            "video_views": 4,
            "cost": 2
        }
        if predefined_statistics is not None:
            base_stats.update(predefined_statistics)
        CampaignStatistic.objects.create(**base_stats)
