from datetime import timedelta, datetime, date

from django.urls import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.tests.pricing_tool.base import PricingToolTestCaseBase
from aw_reporting.api.tests.pricing_tool.base import generate_campaign_statistic
from aw_reporting.api.urls.names import Name
from aw_reporting.models import SalesForceGoalType, Opportunity, OpPlacement, \
    Account, Campaign, AdGroup, GeoTarget, Category, CampaignStatistic, Topic, \
    TopicStatistic, AdGroupStatistic, Audience, AudienceStatistic, \
    VideoCreative, VideoCreativeStatistic, Genders, AgeRanges, \
    Flight, GeoTargeting, device_str, Device
from saas.urls.namespaces import Namespace


class PricingToolCampaignTestCase(PricingToolTestCaseBase):
    _url = reverse(
        Namespace.AW_REPORTING + ":" + Name.PricingTool.CAMPAIGNS)

    def test_pricing_tool_campaign_cpv_client_rate(self):
        start_date, end_date = date(2017, 1, 1), date(2017, 3, 31)
        pl_rate = 1.03
        opportunity, _ = self._create_opportunity_campaign(
            "1",
            opp_data=dict(start=start_date, end=end_date),
            pl_data=dict(ordered_rate=pl_rate,
                         start=start_date, end=end_date),
            goal_type=SalesForceGoalType.CPV)

        response = self._request(campaigns=["campaign_1"])
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
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

        response = self._request(campaigns=["campaign_1"])
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
        self.assertEqual(campaign_data["sf_cpv"], None)
        self.assertEqual(campaign_data["sf_cpm"], pl_rate)

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
        response = self._request(campaigns=["1", "2"])

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        campaigns = response.data

        company_1_data = [c for c in campaigns if c["id"] == campaign_1.id][0]
        company_2_data = [c for c in campaigns if c["id"] == campaign_2.id][0]
        self.assertEqual(company_1_data["start_date"], start_1)
        self.assertEqual(company_1_data["end_date"], end_1)
        self.assertEqual(company_2_data["start_date"], start_2)
        self.assertEqual(company_2_data["end_date"], end_2)

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
        response = self._request(campaigns=["1", "2"])

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        campaigns = response.data
        company_1_data = [c for c in campaigns if c["id"] == campaign_1.id][0]
        company_2_data = [c for c in campaigns if c["id"] == campaign_2.id][0]
        self.assertEqual(company_1_data["budget"], budget_1)
        self.assertEqual(company_2_data["budget"], budget_2)

    def test_campaign_cpm_cpv(self):
        _, campaign = self._create_opportunity_campaign(
            "1", camp_data=dict(cost=123, impressions=2234, video_views=432))
        expected_cpm = campaign.cost / campaign.impressions * 1000
        expected_cpv = campaign.cost / campaign.video_views
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
        self.assertEqual(campaign_data["average_cpm"], expected_cpm)
        self.assertEqual(campaign_data["average_cpv"], expected_cpv)

    def test_campaign_name(self):
        test_name = "Campaign Name"
        self._create_opportunity_campaign("1", camp_data=dict(name=test_name))
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
        self.assertEqual(campaign_data["name"], test_name)

    def test_campaign_apex_deal_true(self):
        self._create_opportunity_campaign("1", opp_data=dict(apex_deal=True))
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
        self.assertTrue(campaign_data["apex_deal"])

    def test_campaign_apex_deal_false(self):
        self._create_opportunity_campaign("1", opp_data=dict(apex_deal=False))
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
        self.assertFalse(campaign_data["apex_deal"])

    def test_campaign_brand(self):
        test_brand = "Test brand 1123"
        self._create_opportunity_campaign("1", opp_data=dict(brand=test_brand))
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
        self.assertEqual(campaign_data["brand"], test_brand)

    def test_campaign_devices(self):
        self._create_opportunity_campaign(
            "1", camp_data=dict(device_computers=True, device_tablets=True))
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
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
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
        self.assertEqual(set(campaign_data["products"]), expected_products)

    def test_campaign_targeting(self):
        self._create_opportunity_campaign(
            "1", camp_data=dict(has_interests=True, has_remarketing=True))
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
        expected_targeting = {"interests", "remarketing"}
        self.assertEqual(set(campaign_data["targeting"]), expected_targeting)

    def test_campaign_demographic(self):
        self._create_opportunity_campaign(
            "1", camp_data=dict(age_18_24=True, gender_female=True))
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
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
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
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
        response = self._request(campaigns=["campaign_1"])

        self.assertEqual(len(response.data), 1)
        campaign_data = response.data[0]
        self.assertEqual(campaign_data["thumbnail"], expected_thumbnail)

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
        response = self._request(campaigns=["campaign_1", "campaign_2"])

        self.assertEqual(response.status_code, HTTP_200_OK)

        campaigns = response.data
        self.assertEqual({c["ctr"] for c in campaigns}, expected_campaigns_ctr)
        self.assertEqual(
            {c["ctr_v"] for c in campaigns}, expected_campaigns_ctr_v)

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

        response = self._request(campaigns=["1", "2"])

        self.assertEqual(response.status_code, HTTP_200_OK)
        margin_by_campaign = {c["id"]: c["margin"]
                              for c in response.data}
        self.assertAlmostEqual(margin_by_campaign[cpm_campaign.id], cpm_margin)
        self.assertAlmostEqual(margin_by_campaign[cpv_campaign.id], cpv_margin)
