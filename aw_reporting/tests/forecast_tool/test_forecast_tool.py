from datetime import date
from datetime import datetime

from django.test import TestCase

from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.tools.forecast_tool.forecast_tool import ForecastTool
from userprofile.models import UserProfile
from utils.datetime import now_in_default_tz
from utils.unittests.recalculate_de_norm_fields import recalculate_de_norm_fields


class ForecastToolTestCase(TestCase):
    def setUp(self):
        self.user = UserProfile.objects.create()

    def test_quarts_to_dates_1(self):
        today = datetime(2017, 4, 1)
        year = today.year
        p_tool = ForecastTool(today=today, quarters=["Q1", "Q2"])
        periods = p_tool.kwargs["periods"]
        self.assertEqual(
            periods,
            [(date(year, 1, 1), date(year, 6, 30))]
        )

    def test_quarts_to_dates_2(self):
        today = datetime(2017, 7, 1)
        year = today.year
        p_tool = ForecastTool(today=today, quarters=["Q2", "Q3"])
        periods = p_tool.kwargs["periods"]
        self.assertEqual(
            periods,
            [(date(year, 4, 1), date(year, 9, 30))]
        )

    def test_quarts_to_dates_3(self):
        today = now_in_default_tz().date()
        today = today.replace(day=1, month=7)
        year = today.year
        p_tool = ForecastTool(quarters=["Q1", "Q3"], today=today)
        periods = p_tool.kwargs["periods"]
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
        p_tool = ForecastTool(quarters=["Q1", "Q4"], today=today)
        periods = p_tool.kwargs["periods"]
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
        p_tool = ForecastTool(quarters=["Q1", "Q2", "Q4"], today=today)
        periods = p_tool.kwargs["periods"]
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
            id="1", name="", start_date=q1_2015,
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

        p_tool = ForecastTool(quarters=["Q1", "Q2"], today=today,
                              compare_yoy=True)
        data = p_tool.estimate

        chart_data = [c for c in data["charts"]["cpm"]["data"]
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
        expected_types = ("Bumper", "Display", "In-stream", "Video discovery")
        product_types = ("", " --", "Standard") + expected_types
        for n, product_type in enumerate(product_types):
            AdGroup.objects.create(id=n, name="", campaign=campaign,
                                   type=product_type)

        filters = ForecastTool.get_filters()
        self.assertEqual(set(e["name"] for e in filters["product_types"]),
                         set(expected_types))

    def test_filter_by_targeting_types(self):

        campaign = Campaign.objects.create(id=1, name="", has_keywords=True)
        Campaign.objects.create(id=2, name="", has_interests=True)

        tool = ForecastTool(targeting_types=["keywords"])
        campaigns = tool.get_campaigns_queryset()
        self.assertEqual(set(_campaign.id for _campaign in campaigns),
                         {campaign.id})

    def test_filter_by_product_types(self):

        campaign1 = Campaign.objects.create(id=1, name="")
        AdGroup.objects.create(id=1, name="", campaign=campaign1,
                               type="Bumper")
        AdGroup.objects.create(id=2, name="", campaign=campaign1,
                               type="Display")
        campaign2 = Campaign.objects.create(id=2, name="")
        AdGroup.objects.create(id=3, name="", campaign=campaign2,
                               type="Display")

        tool = ForecastTool(product_types=["Bumper"])
        campaigns = tool.get_campaigns_queryset()
        self.assertEqual(set(_campaign.id for _campaign in campaigns),
                         {campaign1.id})

    def test_filter_by_age(self):

        campaign1 = Campaign.objects.create(id=1, name="", age_18_24=True)
        campaign2 = Campaign.objects.create(id=2, name="", age_18_24=True, age_25_34=True)
        Campaign.objects.create(id=3, name="", age_25_34=True)

        tool = ForecastTool(ages=[1])
        campaigns = tool.get_campaigns_queryset()
        self.assertEqual(set(_campaign.id for _campaign in campaigns),
                         {campaign1.id, campaign2.id})

    def test_filter_by_gender(self):

        campaign1 = Campaign.objects.create(id=1, name="", gender_male=True)
        campaign2 = Campaign.objects.create(id=2, name="", gender_male=True, gender_female=True)
        Campaign.objects.create(id=3, name="", gender_undetermined=True)

        tool = ForecastTool(genders=[2])
        campaigns = tool.get_campaigns_queryset()
        self.assertEqual(set(_campaign.id for _campaign in campaigns),
                         {campaign1.id, campaign2.id})

    def test_filter_by_parent_status(self):
        Campaign.objects.create(id=1, name="", parent_not_parent=True)
        campaign2 = Campaign.objects.create(id=2, name="", parent_parent=True)
        Campaign.objects.create(id=3, name="", parent_not_parent=True)

        tool = ForecastTool(parents=[0])
        campaigns = tool.get_campaigns_queryset()
        self.assertEqual(set(_campaign.id for _campaign in campaigns),
                         {campaign2.id})

    def test_filter_by_devices(self):
        campaign1 = Campaign.objects.create(id=1, name="", device_computers=True)
        campaign2 = Campaign.objects.create(id=2, name="", device_computers=True, device_tablets=True)
        Campaign.objects.create(id=3, name="", device_tv_screens=True)

        tool = ForecastTool(devices=[0])
        campaigns = tool.get_campaigns_queryset()
        self.assertEqual(set(_campaign.id for _campaign in campaigns),
                         {campaign1.id, campaign2.id})

    def test_filter_by_topics(self):

        campaign1 = Campaign.objects.create(id=1, name="")

        ad_group = AdGroup.objects.create(id=1, campaign=campaign1)
        t_1 = Topic.objects.create(id=1, name="/A 1")
        t_2 = Topic.objects.create(id=2, name="/A 2")
        t_2_1 = Topic.objects.create(id=21, name="/A 2/1", parent=t_2)
        statistic_common = dict(date=date(2017, 1, 1), ad_group=ad_group)
        TopicStatistic.objects.create(topic=t_1, **statistic_common)
        TopicStatistic.objects.create(topic=t_2, **statistic_common)
        TopicStatistic.objects.create(topic=t_2_1, **statistic_common)

        campaign2 = Campaign.objects.create(id=2, name="")
        AdGroup.objects.create(id=2, campaign=campaign2)

        tool = ForecastTool(topics=[t_1.id])
        campaigns = tool.get_campaigns_queryset()
        self.assertEqual(set(_campaign.id for _campaign in campaigns),
                         {campaign1.id})

    def test_filter_by_interests(self):
        campaign1 = Campaign.objects.create(id=1)
        ad_group = AdGroup.objects.create(id=1, campaign=campaign1)
        a_1 = Audience.objects.create(id=1, name="/A 1")
        a_2 = Audience.objects.create(id=2, name="/A 2")
        a_2_1 = Audience.objects.create(id=21, name="/A 2/1", parent=a_2)
        statistic_common = dict(date=date(2017, 1, 1), ad_group=ad_group)
        AudienceStatistic.objects.create(audience=a_1, **statistic_common)
        AudienceStatistic.objects.create(audience=a_2, **statistic_common)
        AudienceStatistic.objects.create(audience=a_2_1, **statistic_common)

        campaign2 = Campaign.objects.create(id=2, name="")
        AdGroup.objects.create(id=2, campaign=campaign2)

        tool = ForecastTool(interests=[a_1.id])
        campaigns = tool.get_campaigns_queryset()
        self.assertEqual(set(_campaign.id for _campaign in campaigns),
                         {campaign1.id})

    def test_filter_by_creative_length(self):
        campaign_1 = Campaign.objects.create(id=1)
        campaign_2 = Campaign.objects.create(id=2)
        Campaign.objects.create(id=3)

        ad_group_1 = AdGroup.objects.create(id=1, name="",
                                            campaign=campaign_1)
        ad_group_2 = AdGroup.objects.create(id=2, name="",
                                            campaign=campaign_1)

        ad_group_3 = AdGroup.objects.create(id=3, name="",
                                            campaign=campaign_2)
        ad_group_4 = AdGroup.objects.create(id=4, name="",
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

        tool = ForecastTool(creative_lengths=[0])
        campaigns = tool.get_campaigns_queryset()
        self.assertEqual(set(_campaign.id for _campaign in campaigns),
                         {campaign_1.id})
