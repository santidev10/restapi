from datetime import date, datetime

from django.test import TestCase

from aw_reporting.models import Opportunity, Campaign, AdGroup, \
    CampaignStatistic, AdGroupStatistic, OpPlacement, Category
from aw_reporting.tasks import recalculate_de_norm_fields
from aw_reporting.tools.pricing_tool import PricingTool
from userprofile.models import UserProfile
from utils.datetime import now_in_default_tz


class PricingToolTestCase(TestCase):
    def setUp(self):
        self.user = UserProfile.objects.create()

    def test_quarts_to_dates_1(self):
        today = datetime(2017, 4, 1)
        year = today.year
        p_tool = PricingTool(today=today, quarters=['Q1', 'Q2'])
        periods = p_tool.kwargs['periods']
        self.assertEqual(
            periods,
            [(date(year, 1, 1), date(year, 6, 30))]
        )

    def test_quarts_to_dates_2(self):
        today = datetime(2017, 7, 1)
        year = today.year
        p_tool = PricingTool(today=today, quarters=['Q2', 'Q3'])
        periods = p_tool.kwargs['periods']
        self.assertEqual(
            periods,
            [(date(year, 4, 1), date(year, 9, 30))]
        )

    def test_quarts_to_dates_3(self):
        today = now_in_default_tz().date()
        today = today.replace(day=1, month=7)
        year = today.year
        p_tool = PricingTool(quarters=['Q1', 'Q3'], today=today)
        periods = p_tool.kwargs['periods']
        self.assertEqual(
            periods,
            [
                (datetime(year, 1, 1).date(), datetime(year, 3, 31).date()),
                (datetime(year, 7, 1).date(), datetime(year, 9, 30).date()),
            ]
        )

    def test_quarts_to_dates_4(self):
        today = now_in_default_tz().date()
        today = today.replace(day=1, month=10)
        year = today.year
        p_tool = PricingTool(quarters=['Q1', 'Q4'], today=today)
        periods = p_tool.kwargs['periods']
        self.assertEqual(
            periods,
            [
                (datetime(year, 1, 1).date(), datetime(year, 3, 31).date()),
                (datetime(year, 10, 1).date(), datetime(year, 12, 31).date()),
            ]
        )

    def test_quarts_to_dates_5(self):
        today = now_in_default_tz().date()
        today = today.replace(day=1, month=10)
        year = today.year
        p_tool = PricingTool(quarters=['Q1', 'Q2', 'Q4'], today=today)
        periods = p_tool.kwargs['periods']
        self.assertEqual(
            periods,
            [
                (datetime(year, 1, 1).date(), datetime(year, 6, 30).date()),
                (datetime(year, 10, 1).date(), datetime(year, 12, 31).date()),
            ]
        )

    def test_estimate_compare_yoy_quarters(self):
        today = datetime(2017, 11, 21).date()
        q1_2015 = datetime(2015, 1, 21).date()
        q2_2015 = datetime(2015, 4, 21).date()
        q1_2016 = q1_2015.replace(year=2016)
        q2_2016 = q2_2015.replace(year=2016)
        q1_2017 = q1_2015.replace(year=2017)
        q2_2017 = q2_2015.replace(year=2017)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)

        campaign = Campaign.objects.create(
            salesforce_placement=placement,
            id="1", name="", start=q1_2015,
        )
        ad_group = AdGroup.objects.create(
            id="1", name="", campaign=campaign, video_views=1
        )
        common = dict(video_views=0, cost=5, impressions=1000, clicks=10)
        campaign_stats = dict(campaign=campaign, **common)
        CampaignStatistic.objects.create(date=q1_2015, **campaign_stats)
        CampaignStatistic.objects.create(date=q2_2015, **campaign_stats)
        CampaignStatistic.objects.create(date=q1_2016, **campaign_stats)
        CampaignStatistic.objects.create(date=q2_2016, **campaign_stats)
        CampaignStatistic.objects.create(date=q1_2017, **campaign_stats)
        CampaignStatistic.objects.create(date=q2_2017, **campaign_stats)
        ag_stats = dict(average_position=1, ad_group=ad_group, **common)
        AdGroupStatistic.objects.create(date=q1_2015, **ag_stats)
        AdGroupStatistic.objects.create(date=q2_2015, **ag_stats)
        AdGroupStatistic.objects.create(date=q1_2016, **ag_stats)
        AdGroupStatistic.objects.create(date=q2_2016, **ag_stats)
        AdGroupStatistic.objects.create(date=q1_2017, **ag_stats)
        AdGroupStatistic.objects.create(date=q2_2017, **ag_stats)

        recalculate_de_norm_fields()

        p_tool = PricingTool(quarters=['Q1', 'Q2'], today=today,
                             compare_yoy=True)
        data = p_tool.estimate

        chart_data = [c for c in data["charts"]["cpm"]['data']
                      if c["label"] in {"2016", "2017"}]

        self.assertEqual(len(chart_data), 2,
                         "IQD-2537: AAU I want YoY timing function to be changed to the last 2 years max")

    def test_filters_products(self):
        """
        IQD-2545 Pricing tool > Remove standard filter option from Product Type filter(BE)
        :return:
        """

        campaign = Campaign.objects.create(
            id="1", name="",
        )
        expected_types = ('Bumper', 'Display', 'In-stream', 'Video discovery')
        for n, product_type in enumerate(
                ('', ' --', 'Standard') + expected_types):
            AdGroup.objects.create(id=n, name="", campaign=campaign,
                                   type=product_type)

        filters = PricingTool.get_filters()
        self.assertEqual(set(e["name"] for e in filters["product_types"]),
                         set(expected_types))

    def test_filter_brands(self):
        """
        IQD-2543 Pricing Tool > Filters option should include only data from existing campaigns (BE)
        :return:
        """
        q1_2015 = datetime(2015, 1, 21).date()
        q2_2015 = datetime(2015, 4, 21).date()

        opportunity_1 = Opportunity.objects.create(id="1", name="",
                                                   brand="Mc.Man")
        placement = OpPlacement.objects.create(id="1", name="",
                                               opportunity=opportunity_1)
        Opportunity.objects.create(id="2", name="", brand="Brandy")
        Opportunity.objects.create(id="3", name="", brand=None)

        campaign = Campaign.objects.create(
            id="1", name="", start=q1_2015,
            salesforce_placement=placement,
        )
        ad_group = AdGroup.objects.create(id="1", name="", campaign=campaign,
                                          video_views=1)
        common = dict(video_views=0, cost=5, impressions=1000, clicks=10)
        campaign_stats = dict(campaign=campaign, **common)
        CampaignStatistic.objects.create(date=q1_2015, **campaign_stats)
        CampaignStatistic.objects.create(date=q2_2015, **campaign_stats)
        ag_stats = dict(average_position=1, ad_group=ad_group, **common)
        AdGroupStatistic.objects.create(date=q1_2015, **ag_stats)
        AdGroupStatistic.objects.create(date=q2_2015, **ag_stats)

        recalculate_de_norm_fields()

        filters = PricingTool.get_filters()
        self.assertEqual(len(filters["brands"]), 1)
        self.assertEqual(filters["brands"][0]["id"], opportunity_1.brand)

    def test_filter_category(self):
        q1_2015 = datetime(2015, 1, 21).date()
        q2_2015 = datetime(2015, 4, 21).date()

        category_1 = Category.objects.create(id="Home & garden")
        category_2 = Category.objects.create(id="Travel")
        opportunity = Opportunity.objects.create(id="1", name="",
                                                 category=category_1)
        placement = OpPlacement.objects.create(id="1", name="",
                                               opportunity=opportunity)
        campaign = Campaign.objects.create(
            id="1", name="", start=q1_2015,
            salesforce_placement=placement,
        )
        ad_group = AdGroup.objects.create(id="1", name="", campaign=campaign,
                                          video_views=1)
        common = dict(video_views=0, cost=5, impressions=1000, clicks=10)
        campaign_stats = dict(campaign=campaign, **common)
        CampaignStatistic.objects.create(date=q1_2015, **campaign_stats)
        CampaignStatistic.objects.create(date=q2_2015, **campaign_stats)
        ag_stats = dict(average_position=1, ad_group=ad_group, **common)
        AdGroupStatistic.objects.create(date=q1_2015, **ag_stats)
        AdGroupStatistic.objects.create(date=q2_2015, **ag_stats)

        recalculate_de_norm_fields()

        filters = PricingTool.get_filters()
        self.assertEqual(len(filters["categories"]), 1)
        self.assertEqual(filters["categories"][0]["id"], category_1.id)
