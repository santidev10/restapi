import json
from datetime import timedelta, datetime, date

from django.utils import timezone
from rest_framework.reverse import reverse
from rest_framework.status import HTTP_200_OK, \
    HTTP_401_UNAUTHORIZED

from aw_reporting.api.urls.names import Name
from aw_reporting.models import AdGroupStatistic, CampaignStatistic, AdGroup, \
    Campaign, Account, Opportunity, OpPlacement, SalesForceGoalType
from aw_reporting.tasks import recalculate_de_norm_fields
from saas.urls.namespaces import Namespace
from utils.datetime import now_in_default_tz
from utils.utils_tests import ExtendedAPITestCase, patch_now, \
    patch_instance_settings


class PricingToolEstimateTestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.AW_REPORTING + ":" + Name.PricingTool.ESTIMATE)

    def _request(self, **kwargs):
        return self.client.post(self._url, json.dumps(kwargs),
                                content_type="application/json")

    def setUp(self):
        self.user = self.create_test_user()

    def test_failed_access(self):
        self.user.delete()

        response = self._request()
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_pricing_tool_estimate(self):
        today = timezone.now().date()
        account = Account.objects.create(id="1", name="")
        campaign = Campaign.objects.create(
            id="1", name="Campaign name", account=account,
        )
        ad_group = AdGroup.objects.create(
            id="1", name="", campaign=campaign, type="Bumper", video_views=2,
        )
        CampaignStatistic.objects.create(
            campaign=campaign, date=today, impressions=10, video_views=4,
            cost=2,
        )
        AdGroupStatistic.objects.create(
            ad_group=ad_group, date=today, impressions=10, video_views=4,
            cost=2, average_position=1,
        )

        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {'average_cpm', 'charts', 'average_cpv', 'suggested_cpm', 'margin',
             'suggested_cpv'}
        )

    def test_pricing_tool_estimate_empty(self):
        """
        It is possible that
        there are ad-groups that meet selected filters
        and there is no campaigns that meet selected filters
        both campaigns and estimate responses should be empty
        :return:
        """

        today = timezone.now().date()
        account = Account.objects.create(id="1", name="")
        campaign = Campaign.objects.create(
            id="1", name="Campaign name", account=account,
        )
        ad_group = AdGroup.objects.create(
            id="1", name="", campaign=campaign, type="Bumper", video_views=2,
        )
        CampaignStatistic.objects.create(
            campaign=campaign, date=today, impressions=10, video_views=4,
            cost=2,
        )
        AdGroupStatistic.objects.create(
            ad_group=ad_group, date=today, impressions=10, video_views=4,
            cost=2, average_position=1,
        )

        request_data = dict(
            product_types=["Bumper", "Display"],
            product_types_condition="and",
        )

        response = self._request(**request_data)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNone(response.data["average_cpm"])
        self.assertIsNone(response.data["average_cpv"])

    def test_pricing_filter_creative_length_5_no_error(self):
        today = timezone.now().date()
        account = Account.objects.create(id="1", name="")
        campaign = Campaign.objects.create(
            id="1", name="Campaign name", account=account,
        )
        ad_group = AdGroup.objects.create(
            id="1", name="", campaign=campaign, type="Bumper", video_views=2,
        )
        CampaignStatistic.objects.create(
            campaign=campaign, date=today, impressions=10, video_views=4,
            cost=2,
        )
        AdGroupStatistic.objects.create(
            ad_group=ad_group, date=today, impressions=10, video_views=4,
            cost=2, average_position=1,
        )

        response = self._request(creative_lengths=[5])
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_estimate_compare_yoy(self):
        """
        Compare YoY will show multiple overlapping lines on the same graph
        of other years in the same timing
        :return:
        """
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)

        now = datetime(2018, 2, 20)
        start, end = date(2017, 10, 1), date(2018, 3, 31)

        campaign = Campaign.objects.create(
            salesforce_placement=placement,
            id="1", name=""
        )
        CampaignStatistic.objects.create(
            campaign=campaign, date=start, impressions=10, video_views=4,
            cost=2,
        )
        ad_group = AdGroup.objects.create(
            id="1", name="", campaign=campaign, video_views=1
        )
        common = dict(average_position=1, ad_group=ad_group)
        AdGroupStatistic.objects.create(date=start, video_views=100, cost=15,
                                        **common)
        AdGroupStatistic.objects.create(date=start + timedelta(days=30),
                                        video_views=100, cost=25, **common)
        AdGroupStatistic.objects.create(date=end - timedelta(days=2),
                                        video_views=100, cost=25, **common)
        AdGroupStatistic.objects.create(date=end, video_views=100, cost=15,
                                        **common)
        # these two below won't be included to the results
        AdGroupStatistic.objects.create(date=end + timedelta(days=1),
                                        video_views=10, cost=150, **common)
        AdGroupStatistic.objects.create(date=start - timedelta(days=1),
                                        video_views=10, cost=250, **common)

        recalculate_de_norm_fields()

        with patch_now(now):
            response = self._request(quarters=["Q1", "Q4"],
                                     compare_yoy=True)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data

        self.assertIsNotNone(
            data["charts"]["cpv"])  # CPV this year and CPV next year
        self.assertIsNone(data["charts"]["cpm"])

        cpv_chart = data["charts"]["cpv"]
        self.assertEqual(cpv_chart["title"], "CPV")
        self.assertEqual(cpv_chart["data"][0]["label"],
                         "{}".format(start.year))
        self.assertEqual(len(cpv_chart["data"][0]["trend"]), 2)
        self.assertEqual(
            tuple(i["value"] for i in cpv_chart["data"][0]["trend"]),
            (.15, .25))

        self.assertEqual(cpv_chart["data"][1]["label"], "{}".format(end.year))
        self.assertEqual(len(cpv_chart["data"][1]["trend"]), 2)
        self.assertEqual(
            tuple(i["value"] for i in cpv_chart["data"][1]["trend"]),
            (.25, .15))

    def test_estimate_filter_quarter(self):
        today = datetime(2017, 12, 1)
        today_date = today.date()
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)

        q1_start = today_date.replace(day=1, month=1)
        q1_end = today_date.replace(day=31, month=3)

        q3_start = today_date.replace(day=1, month=7)
        q3_end = today_date.replace(day=30, month=9)
        if today_date.month < 7:
            q3_start = q3_start.replace(year=today_date.year - 1)
            q3_end = q3_end.replace(year=today_date.year - 1)

        campaign = Campaign.objects.create(
            id="1", name="",
            salesforce_placement=placement)
        ad_group = AdGroup.objects.create(
            id="1", name="", campaign=campaign,
        )
        CampaignStatistic.objects.create(campaign=campaign, date=q1_start)
        CampaignStatistic.objects.create(campaign=campaign, date=q3_end)
        common = dict(average_position=1, ad_group=ad_group)
        AdGroupStatistic.objects.create(date=q1_start, impressions=1000,
                                        cost=15, **common)
        AdGroupStatistic.objects.create(date=q1_end, impressions=1000, cost=15,
                                        **common)
        AdGroupStatistic.objects.create(date=q3_start + timedelta(days=30),
                                        impressions=1000, cost=25, **common)
        AdGroupStatistic.objects.create(date=q3_end, impressions=1000, cost=25,
                                        **common)
        # these two below won't be included to the results
        AdGroupStatistic.objects.create(date=q1_end + timedelta(days=1),
                                        impressions=100, cost=150, **common)
        AdGroupStatistic.objects.create(date=q3_start - timedelta(days=1),
                                        impressions=100, cost=250, **common)

        recalculate_de_norm_fields()

        with patch_now(today):
            response = self._request(quarters=["Q1", "Q3"],
                                     margin=50)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            {
                'average_cpv', 'average_cpm',
                'suggested_cpm', 'suggested_cpv', 'margin',
                'charts',
            }
        )
        self.assertEqual(data["average_cpm"], 20)
        self.assertEqual(data["margin"], 50)
        self.assertEqual(data["suggested_cpm"], 40)
        self.assertEqual(data["charts"]["cpm"]["title"], "CPM")
        cpm_charts = [c for c in data["charts"]["cpm"]["data"]
                      if c["label"] == "CPM"]
        self.assertEqual(len(cpm_charts), 1)
        cpm_chart = cpm_charts[0]
        self.assertEqual(len(cpm_chart["trend"]), 4)
        self.assertEqual(tuple(
            i["value"] for i in cpm_chart["trend"]),
            (15, 15, 25, 25))

    def test_estimate_filter_ctr(self):
        opportunity_1 = Opportunity.objects.create(id="1")
        placement_1 = OpPlacement.objects.create(id="1",
                                                 opportunity=opportunity_1)
        opportunity_2 = Opportunity.objects.create(id="2")
        placement_2 = OpPlacement.objects.create(id="2",
                                                 opportunity=opportunity_2)

        campaign_1 = Campaign.objects.create(
            id="1", name="",
            salesforce_placement=placement_1)
        campaign_2 = Campaign.objects.create(
            id="2", name="",
            salesforce_placement=placement_2)

        ad_group_1 = AdGroup.objects.create(
            id="1", name="", campaign=campaign_1)
        ad_group_2 = AdGroup.objects.create(
            id="2", name="", campaign=campaign_2)
        today = now_in_default_tz().date()

        CampaignStatistic.objects.create(campaign=campaign_1, date=today,
                                         clicks=20, impressions=1000)
        CampaignStatistic.objects.create(campaign=campaign_2, date=today,
                                         clicks=40, impressions=1000)
        common = dict(average_position=1, impressions=1000)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1, cost=15, date=today, **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1, cost=20,
            date=today + timedelta(days=1), **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1, cost=25,
            date=today + timedelta(days=2), **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1, cost=30,
            date=today + timedelta(days=3), **common)
        # these tree below won't be included to the results
        AdGroupStatistic.objects.create(
            ad_group=ad_group_2, cost=10, date=today, **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_2, cost=40,
            date=today + timedelta(days=1), **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_2, cost=60,
            date=today + timedelta(days=2), **common)
        recalculate_de_norm_fields()

        response = self._request(
            max_ctr=3.,
            min_ctr=1.5,
            start=str(today),
            end=str(today + timedelta(days=30))
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["average_cpm"], 22.5)
        self.assertEqual(data["charts"]["cpm"]["title"], "CPM")
        cpm_charts = [c for c in data["charts"]["cpm"]["data"]
                      if c["label"] == "CPM"]
        self.assertEqual(len(cpm_charts), 1)
        cpm_chart = cpm_charts[0]
        self.assertEqual(len(cpm_chart["trend"]), 4)
        self.assertEqual(
            tuple(
                i["value"] for i in cpm_chart["trend"]),
            (15, 20, 25, 30))

    def test_estimate_filter_ctr_v(self):
        today = now_in_default_tz().date()
        start, end = today, today + timedelta(days=30)
        opportunity_1 = Opportunity.objects.create(id="1")
        placement_1 = OpPlacement.objects.create(id="1",
                                                 opportunity=opportunity_1)
        opportunity_2 = Opportunity.objects.create(id="2")
        placement_2 = OpPlacement.objects.create(id="2",
                                                 opportunity=opportunity_2)

        campaign_1 = Campaign.objects.create(
            id="1", name="",
            salesforce_placement=placement_1)
        campaign_2 = Campaign.objects.create(
            id="2", name="",
            salesforce_placement=placement_2)

        ad_group_1 = AdGroup.objects.create(
            id="1", name="", campaign=campaign_1, video_views=1)
        ad_group_2 = AdGroup.objects.create(
            id="2", name="", campaign=campaign_2, video_views=1)

        CampaignStatistic.objects.create(campaign=campaign_1, date=today,
                                         clicks=20, video_views=1000)
        CampaignStatistic.objects.create(campaign=campaign_2, date=today,
                                         clicks=40, video_views=1000)
        common = dict(average_position=1, video_views=1000)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1, cost=15, date=today, **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1, cost=20,
            date=today + timedelta(days=1), **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1, cost=25,
            date=today + timedelta(days=2), **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1, cost=30,
            date=today + timedelta(days=3), **common)
        # these tree below won't be included to the results
        AdGroupStatistic.objects.create(
            ad_group=ad_group_2, cost=10, date=today, **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_2, cost=40,
            date=today + timedelta(days=1), **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_2, cost=60,
            date=today + timedelta(days=2), **common)
        recalculate_de_norm_fields()

        response = self._request(
            max_ctr_v=3., min_ctr_v=1.5,
            start=str(start), end=str(end)
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["average_cpv"], 0.0225)
        self.assertEqual(data["charts"]["cpv"]["title"], "CPV")
        cpv_charts = [c for c in data["charts"]["cpv"]["data"]
                      if c["label"] == "CPV"]
        self.assertEqual(len(cpv_charts), 1)
        cpv_chart = cpv_charts[0]
        self.assertEqual(len(cpv_chart["trend"]), 4)
        self.assertEqual(tuple(
            i["value"] for i in cpv_chart["trend"]),
            (0.015, 0.02, 0.025, 0.03))

    def test_estimate_filter_view_rate(self):
        opportunity_1 = Opportunity.objects.create(id="1")
        placement_1 = OpPlacement.objects.create(
            id="1", opportunity=opportunity_1,
            goal_type_id=SalesForceGoalType.CPV)
        opportunity_2 = Opportunity.objects.create(id="2")
        placement_2 = OpPlacement.objects.create(
            id="2", opportunity=opportunity_2,
            goal_type_id=SalesForceGoalType.CPV)

        campaign_1 = Campaign.objects.create(
            id="1", name="",
            cost=20, salesforce_placement=placement_1)
        campaign_2 = Campaign.objects.create(
            id="2", name="",
            cost=40, salesforce_placement=placement_2)

        ad_group_1 = AdGroup.objects.create(
            id="1", name="", campaign=campaign_1, video_views=1)
        ad_group_2 = AdGroup.objects.create(
            id="2", name="", campaign=campaign_2, video_views=1)
        today = now_in_default_tz().date()

        CampaignStatistic.objects.create(campaign=campaign_1, date=today,
                                         video_views=200, impressions=1000)
        CampaignStatistic.objects.create(campaign=campaign_2, date=today,
                                         video_views=400, impressions=1000)
        common = dict(average_position=1, impressions=1000)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1, date=today, cost=100, **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1,
            date=today + timedelta(days=1), cost=200, **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1,
            date=today + timedelta(days=2), cost=300, **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1,
            date=today + timedelta(days=3), cost=400, **common)
        # these tree below won't be included to the results
        AdGroupStatistic.objects.create(
            ad_group=ad_group_2, date=today, cost=500, **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_2,
            date=today + timedelta(days=1), cost=600, **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_2,
            date=today + timedelta(days=2), cost=700, **common)
        recalculate_de_norm_fields()

        response = self._request(
            max_video_view_rate=30, min_video_view_rate=10,
            margin=50, start=str(today),
            end=str(today + timedelta(days=30))
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["average_cpm"], 250)
        self.assertEqual(data["suggested_cpm"], 500)
        self.assertEqual(data["charts"]["cpm"]["title"], "CPM")
        cpm_charts = [c for c in data["charts"]["cpm"]["data"]
                      if c["label"] == "CPM"]
        self.assertEqual(len(cpm_charts), 1)
        cpm_chart = cpm_charts[0]
        self.assertEqual(len(cpm_chart["trend"]), 4)
        self.assertEqual(
            tuple(
                i["value"] for i in cpm_chart["trend"]),
            (100, 200, 300, 400))

    def test_estimate_filter_video100rate(self):
        opportunity_1 = Opportunity.objects.create(id="1")
        placement_1 = OpPlacement.objects.create(id="1",
                                                 opportunity=opportunity_1)
        opportunity_2 = Opportunity.objects.create(id="2")
        placement_2 = OpPlacement.objects.create(id="2",
                                                 opportunity=opportunity_2)

        campaign_common = dict(name="", impressions=100)
        campaign_1 = Campaign.objects.create(
            id="1", video_views_100_quartile=20,
            salesforce_placement=placement_1,
            **campaign_common)
        # following campaign should be excluded
        campaign_2 = Campaign.objects.create(
            id="2", video_views_100_quartile=40,
            salesforce_placement=placement_2,
            **campaign_common)

        ad_group_1 = AdGroup.objects.create(
            id="1", name="", campaign=campaign_1)
        ad_group_2 = AdGroup.objects.create(
            id="2", name="", campaign=campaign_2)

        today = now_in_default_tz().date()
        common = dict(
            average_position=1, date=today, cost=100, video_views=100)
        CampaignStatistic.objects.create(campaign=campaign_1, date=today,
                                         impressions=100,
                                         video_views_100_quartile=20)
        CampaignStatistic.objects.create(campaign=campaign_2, date=today,
                                         impressions=100,
                                         video_views_100_quartile=40)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_1, impressions=1000, **common)
        AdGroupStatistic.objects.create(
            ad_group=ad_group_2, impressions=2000, **common)

        recalculate_de_norm_fields()

        response = self._request(
            max_video100rate=30, mix_video100rate=10,
            margin=50, start=str(today), end=str(today)
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["average_cpm"], 100)
        self.assertEqual(data["suggested_cpm"], 200)
        cpm_charts = [c for c in data["charts"]["cpm"]["data"]
                      if c["label"] == "CPM"]
        self.assertEqual(len(cpm_charts), 1)
        cpm_chart = cpm_charts[0]

        self.assertEqual(len(cpm_chart["trend"]), 1)
        self.assertEqual(
            tuple(
                i["value"] for i in cpm_chart["trend"]),
            (100,))

    def test_estimate_exclude_filters(self):
        today = now_in_default_tz().date()
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)

        campaign_included = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement)
        campaign_excluded = Campaign.objects.create(
            id="2", name="", salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign_included,
                                         date=today)
        CampaignStatistic.objects.create(campaign=campaign_excluded,
                                         date=today)

        ag_1_included = AdGroup.objects.create(id="1", name="",
                                               campaign=campaign_included)
        ag_1_excluded = AdGroup.objects.create(id="2", name="",
                                               campaign=campaign_included)
        ag_2_included = AdGroup.objects.create(id="3", name="",
                                               campaign=campaign_excluded)
        ag_2_excluded = AdGroup.objects.create(id="4", name="",
                                               campaign=campaign_excluded)

        common = dict(average_position=1, impressions=1000, date=today)

        AdGroupStatistic.objects.create(ad_group=ag_1_included, cost=15,
                                        **common)
        AdGroupStatistic.objects.create(ad_group=ag_1_excluded, cost=50,
                                        **common)
        AdGroupStatistic.objects.create(ad_group=ag_2_included, cost=75,
                                        **common)
        AdGroupStatistic.objects.create(ad_group=ag_2_excluded, cost=100,
                                        **common)

        recalculate_de_norm_fields()

        response = self._request(
            start=str(today), end=str(today),
            exclude_campaigns=[campaign_excluded.id],
            exclude_ad_groups=[ag_1_excluded.id,
                               ag_2_excluded.id]
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["average_cpm"], 15)

    def test_estimate_exclude_filters_opportunities(self):
        today = now_in_default_tz().date()

        opportunity_include = Opportunity.objects.create(id="1")
        opportunity_exclude = Opportunity.objects.create(id="2")
        placement_include = OpPlacement.objects.create(
            id="1", opportunity=opportunity_include)
        placement_exclude = OpPlacement.objects.create(
            id="2", opportunity=opportunity_exclude)
        campaign_included = Campaign.objects.create(
            id="1", name="",
            salesforce_placement=placement_include)
        campaign_excluded = Campaign.objects.create(
            id="2", name="",
            salesforce_placement=placement_exclude)
        CampaignStatistic.objects.create(campaign=campaign_included,
                                         date=today)
        CampaignStatistic.objects.create(campaign=campaign_excluded,
                                         date=today)

        ag_1_included = AdGroup.objects.create(id="1", name="",
                                               campaign=campaign_included)
        ag_1_excluded = AdGroup.objects.create(id="2", name="",
                                               campaign=campaign_excluded)

        common = dict(average_position=1, impressions=1000, date=today)

        AdGroupStatistic.objects.create(ad_group=ag_1_included, cost=15,
                                        **common)
        AdGroupStatistic.objects.create(ad_group=ag_1_excluded, cost=50,
                                        **common)

        recalculate_de_norm_fields()

        response = self._request(exclude_opportunities=[opportunity_exclude.id])
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["average_cpm"], 15)

    def test_estimate_product_types(self):
        today = now_in_default_tz().date()
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)

        campaign = Campaign.objects.create(id="1", name="",
                                           salesforce_placement=placement)
        CampaignStatistic.objects.create(campaign=campaign, date=today)

        ad_group_1 = AdGroup.objects.create(id="1", name="", campaign=campaign,
                                            type="Bumper Ad")
        ad_group_2 = AdGroup.objects.create(id="2", name="", campaign=campaign,
                                            type="In-stream")

        common = dict(average_position=1, impressions=1000, date=today)
        AdGroupStatistic.objects.create(ad_group=ad_group_1, cost=15, **common)
        AdGroupStatistic.objects.create(ad_group=ad_group_2, cost=50, **common)

        recalculate_de_norm_fields()

        response = self._request(
            start=str(today), end=str(today),
            product_types=["Bumper Ad"]
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["average_cpm"], (15 + 50) / 2.)

    def test_missing_campaigns_do_not_affect_chart(self):
        today = now_in_default_tz().date()
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(
            opportunity=opportunity, goal_type_id=SalesForceGoalType.CPV)
        campaign = Campaign.objects.create(salesforce_placement=placement,
                                           start_date=today, end_date=today,
                                           video_views=1)
        ad_group = AdGroup.objects.create(campaign=campaign, video_views=1)
        AdGroupStatistic.objects.create(ad_group=ad_group,
                                        date=today, cost=1,
                                        video_views=1,
                                        average_position=1)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today, cost=1,
                                         video_views=1)
        recalculate_de_norm_fields()

        response = self._request(start=str(today), end=str(today))
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data

        campaign = Campaign.objects.create(id="2",
                                           start_date=today, end_date=today,
                                           video_views=2)
        ad_group = AdGroup.objects.create(id="2",
                                          campaign=campaign, video_views=2)
        AdGroupStatistic.objects.create(ad_group=ad_group,
                                        date=today, cost=1,
                                        video_views=2,
                                        average_position=1)
        CampaignStatistic.objects.create(campaign=campaign,
                                         date=today, cost=1,
                                         video_views=2)
        recalculate_de_norm_fields()

        response = self._request(start=str(today), end=str(today))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, data)

    def test_filter_by_product_type_with_and_condition(self):
        """
        Bug: https://channelfactory.atlassian.net/browse/IQD-2690
        Summary: Test > No data in estimate/ end-point if user select
        Product type: bumper and In-stream by AND logic
        """

        opportunity = Opportunity.objects.create()
        stats_date = datetime(2017, 1, 1)
        placement_1 = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV)
        placement_2 = OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM)
        campaign_1 = Campaign.objects.create(id="1",
                                             min_stat_date=stats_date,
                                             max_stat_date=stats_date,
                                             salesforce_placement=placement_1)
        campaign_2 = Campaign.objects.create(id="2",
                                             min_stat_date=stats_date,
                                             max_stat_date=stats_date,
                                             salesforce_placement=placement_2)
        type_1 = "Bumper"
        type_2 = "In-stream"
        ad_group_1 = AdGroup.objects.create(
            id="1", campaign=campaign_1, type=type_1, video_views=1)
        ad_group_2 = AdGroup.objects.create(
            id="2", campaign=campaign_2, type=type_2)

        AdGroupStatistic.objects.create(ad_group=ad_group_1, date=stats_date,
                                        cost=1, video_views=1,
                                        average_position=1)
        AdGroupStatistic.objects.create(ad_group=ad_group_2, date=stats_date,
                                        cost=1, impressions=1,
                                        average_position=1)

        with patch_now(stats_date):
            response = self._request(product_types=[type_1, type_2],
                                     product_types_condition="and")

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(response.data["charts"]["cpm"])
        self.assertIsNotNone(response.data["charts"]["cpv"])

    def test_filter_hidden_campaigns(self):
        opportunity = Opportunity.objects.create()
        stats_date = datetime(2017, 1, 1)
        placement_1 = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV)
        placement_2 = OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM)
        account = Account.objects.create(id="id")
        campaign_1 = Campaign.objects.create(id="1",
                                             account=account,
                                             min_stat_date=stats_date,
                                             max_stat_date=stats_date,
                                             salesforce_placement=placement_1)
        campaign_2 = Campaign.objects.create(id="2",
                                             account=account,
                                             min_stat_date=stats_date,
                                             max_stat_date=stats_date,
                                             salesforce_placement=placement_2)
        ad_group_1 = AdGroup.objects.create(
            id="1", campaign=campaign_1, video_views=1)
        ad_group_2 = AdGroup.objects.create(
            id="2", campaign=campaign_2)

        AdGroupStatistic.objects.create(ad_group=ad_group_1, date=stats_date,
                                        cost=1, video_views=1,
                                        average_position=1)
        AdGroupStatistic.objects.create(ad_group=ad_group_2, date=stats_date,
                                        cost=1, impressions=1,
                                        average_position=1)

        with patch_now(stats_date), \
             patch_instance_settings(visible_accounts=[]):
            response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNone(response.data["average_cpm"])
        self.assertIsNone(response.data["average_cpv"])
        self.assertIsNone(response.data["suggested_cpm"])
        self.assertIsNone(response.data["suggested_cpv"])
        self.assertIsNone(response.data["charts"]["cpm"])
        self.assertIsNone(response.data["charts"]["cpv"])

    def test_does_not_filter_campaigns_without_account(self):
        opportunity = Opportunity.objects.create()
        stats_date = datetime(2017, 1, 1)
        placement_1 = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPV)
        placement_2 = OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            goal_type_id=SalesForceGoalType.CPM)
        campaign_1 = Campaign.objects.create(id="1",
                                             min_stat_date=stats_date,
                                             max_stat_date=stats_date,
                                             salesforce_placement=placement_1)
        campaign_2 = Campaign.objects.create(id="2",
                                             min_stat_date=stats_date,
                                             max_stat_date=stats_date,
                                             salesforce_placement=placement_2)
        ad_group_1 = AdGroup.objects.create(
            id="1", campaign=campaign_1, video_views=1)
        ad_group_2 = AdGroup.objects.create(
            id="2", campaign=campaign_2)

        AdGroupStatistic.objects.create(ad_group=ad_group_1, date=stats_date,
                                        cost=1, video_views=1,
                                        average_position=1)
        AdGroupStatistic.objects.create(ad_group=ad_group_2, date=stats_date,
                                        cost=1, impressions=1,
                                        average_position=1)

        with patch_now(stats_date), \
             patch_instance_settings(visible_accounts=[]):
            response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["average_cpm"], 2000.)
        self.assertEqual(response.data["average_cpv"], 1.)
        self.assertAlmostEqual(response.data["suggested_cpm"], 2000. / 0.7)
        self.assertAlmostEqual(response.data["suggested_cpv"], 1. / 0.7)
        self.assertIsNotNone(response.data["charts"]["cpm"])
        self.assertIsNotNone(response.data["charts"]["cpv"])

    def test_planned_cpm(self):
        opportunity = Opportunity.objects.create()
        now = datetime(2017, 1, 1)
        start_1, end_1 = date(2017, 1, 1), date(2017, 1, 2)
        start_2, end_2 = date(2017, 1, 2), date(2017, 1, 2)
        client_cost_1, ordered_units_1 = 1234, 124545
        client_cost_2, ordered_units_2 = 255, 65434
        expected_planned_cpm = (
            client_cost_1 * 1000. / ordered_units_1,
            (client_cost_1 + client_cost_2) * 1000. / (
                    ordered_units_1 + ordered_units_2),
        )
        placement_1 = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            start=start_1, end=end_1,
            total_cost=client_cost_1, ordered_units=ordered_units_1,
            goal_type_id=SalesForceGoalType.CPM)
        placement_2 = OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            start=start_2, end=end_2,
            total_cost=client_cost_2, ordered_units=ordered_units_2,
            goal_type_id=SalesForceGoalType.CPM)
        Campaign.objects.create(id="1",
                                salesforce_placement=placement_1,
                                start_date=start_1, end_date=end_1)
        Campaign.objects.create(id="2",
                                salesforce_placement=placement_2,
                                start_date=start_2, end_date=end_2)

        with patch_now(now):
            response = self._request(start=str(start_1), end=str(end_1))

        self.assertEqual(response.status_code, HTTP_200_OK)
        planned_cpm_trends = [c for c in response.data["charts"]["cpm"]["data"]
                              if c["label"] == "Planned CPM"][0]["trend"]
        self.assertIsNone(response.data["charts"]["cpv"])
        self.assertAlmostEqual(planned_cpm_trends[0]["value"],
                               expected_planned_cpm[0])
        self.assertAlmostEqual(planned_cpm_trends[1]["value"],
                               expected_planned_cpm[1])

    def test_planned_cpv(self):
        opportunity = Opportunity.objects.create()
        now = datetime(2017, 1, 1)
        start_1, end_1 = date(2017, 1, 1), date(2017, 1, 2)
        start_2, end_2 = date(2017, 1, 2), date(2017, 1, 2)
        client_cost_1, ordered_units_1 = 1234, 124545
        client_cost_2, ordered_units_2 = 255, 65434
        expected_planned_cpv = (
            client_cost_1 / ordered_units_1,
            (client_cost_1 + client_cost_2) / (
                    ordered_units_1 + ordered_units_2),
        )
        placement_1 = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            start=start_1, end=end_1,
            total_cost=client_cost_1, ordered_units=ordered_units_1,
            goal_type_id=SalesForceGoalType.CPV)
        placement_2 = OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            start=start_2, end=end_2,
            total_cost=client_cost_2, ordered_units=ordered_units_2,
            goal_type_id=SalesForceGoalType.CPV)
        Campaign.objects.create(id="1",
                                salesforce_placement=placement_1,
                                start_date=start_1, end_date=end_1)
        Campaign.objects.create(id="2",
                                salesforce_placement=placement_2,
                                start_date=start_2, end_date=end_2)

        with patch_now(now):
            response = self._request(start=str(start_1), end=str(end_1))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNone(response.data["charts"]["cpm"])
        planned_cpv_trend = [c for c in response.data["charts"]["cpv"]["data"]
                             if c["label"] == "Planned CPV"][0]["trend"]
        self.assertAlmostEqual(planned_cpv_trend[0]["value"],
                               expected_planned_cpv[0])
        self.assertAlmostEqual(planned_cpv_trend[1]["value"],
                               expected_planned_cpv[1])

    def test_planned_stats_filters_hidden_accounts(self):
        opportunity = Opportunity.objects.create()
        now = datetime(2017, 1, 1)
        start = end = date(2017, 1, 1)
        placement_1 = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            start=start, end=end,
            total_cost=99999, ordered_units=99999,
            goal_type_id=SalesForceGoalType.CPV)
        placement_2 = OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            start=start, end=end,
            total_cost=99999, ordered_units=99999,
            goal_type_id=SalesForceGoalType.CPM)
        cpv_cost, cpm_cost = 123, 234
        cpv_units, cpm_units = 123, 234
        placement_3 = OpPlacement.objects.create(
            id="3", opportunity=opportunity,
            start=start, end=end,
            total_cost=cpv_cost, ordered_units=cpv_units,
            goal_type_id=SalesForceGoalType.CPV)
        placement_4 = OpPlacement.objects.create(
            id="4", opportunity=opportunity,
            start=start, end=end,
            total_cost=cpm_cost, ordered_units=cpm_units,
            goal_type_id=SalesForceGoalType.CPM)
        account = Account.objects.create()
        Campaign.objects.create(id="1", account=account,
                                salesforce_placement=placement_1,
                                start_date=start, end_date=end)
        Campaign.objects.create(id="2", account=account,
                                salesforce_placement=placement_2,
                                start_date=start, end_date=end)
        Campaign.objects.create(id="3",
                                salesforce_placement=placement_3,
                                start_date=start, end_date=end)
        Campaign.objects.create(id="4",
                                salesforce_placement=placement_4,
                                start_date=start, end_date=end)

        with patch_now(now), patch_instance_settings(visible_accounts=[]):
            response = self._request(start=str(start), end=str(end))

        self.assertEqual(response.status_code, HTTP_200_OK)
        planned_cpm = [c for c in response.data["charts"]["cpm"]["data"]
                       if c["label"] == "Planned CPM"][0]["trend"]
        planned_cpv = [c for c in response.data["charts"]["cpv"]["data"]
                       if c["label"] == "Planned CPV"][0]["trend"]
        self.assertAlmostEqual(planned_cpm[0]["value"],
                               cpm_cost * 1000. / cpm_units)
        self.assertAlmostEqual(planned_cpv[0]["value"],
                               cpv_cost * 1. / cpv_units)

    def test_planned_stats_compare_yoy(self):
        opportunity = Opportunity.objects.create()
        now = datetime(2017, 1, 1)
        start, end = date(2016, 1, 1), date(2017, 1, 1)
        cpv_cost, cpm_cost = 123, 234
        cpv_units, cpm_units = 123, 234
        placement_1 = OpPlacement.objects.create(
            id="1", opportunity=opportunity,
            start=start, end=end,
            total_cost=cpv_cost, ordered_units=cpv_units,
            goal_type_id=SalesForceGoalType.CPV)
        placement_2 = OpPlacement.objects.create(
            id="2", opportunity=opportunity,
            start=start, end=end,
            total_cost=cpm_cost, ordered_units=cpm_units,
            goal_type_id=SalesForceGoalType.CPM)
        Campaign.objects.create(id="1",
                                salesforce_placement=placement_1,
                                start_date=start, end_date=end)
        Campaign.objects.create(id="2",
                                salesforce_placement=placement_2,
                                start_date=start, end_date=end)

        with patch_now(now):
            response = self._request(start=str(start), end=str(end),
                                     compare_yoy=True)

        self.assertEqual(response.status_code, HTTP_200_OK)
        planned_cpm_last = [c for c in response.data["charts"]["cpm"]["data"]
                            if c["label"] == "Planned 2016"][0]["trend"]
        planned_cpm_current = [c for c in response.data["charts"]["cpm"]["data"]
                               if c["label"] == "Planned 2017"][0]["trend"]
        planned_cpv_last = [c for c in response.data["charts"]["cpv"]["data"]
                            if c["label"] == "Planned 2016"][0]["trend"]
        planned_cpv_current = [c for c in response.data["charts"]["cpv"]["data"]
                               if c["label"] == "Planned 2017"][0]["trend"]
        self.assertAlmostEqual(planned_cpm_last[0]["value"],
                               cpm_cost * 1000. / cpm_units)
        self.assertAlmostEqual(planned_cpm_current[0]["value"],
                               cpm_cost * 1000. / cpm_units)
        self.assertAlmostEqual(planned_cpv_last[0]["value"],
                               cpv_cost * 1. / cpv_units)
        self.assertAlmostEqual(planned_cpv_current[0]["value"],
                               cpv_cost * 1. / cpv_units)
