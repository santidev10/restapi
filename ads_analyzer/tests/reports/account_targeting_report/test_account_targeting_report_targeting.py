from datetime import date
from datetime import timedelta

from django.test import TransactionTestCase

from ads_analyzer.reports.account_targeting_report import constants as names
from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupTargeting
from aw_reporting.models import AgeRange
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CriteriaType
from aw_reporting.models import CriteriaTypeEnum
from aw_reporting.models import Gender
from aw_reporting.models import GenderStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import Parent
from aw_reporting.models import ParentStatistic
from aw_reporting.models import RemarkList
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import TargetingStatusEnum
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from utils.unittests.int_iterator import int_iterator

AGGREGATIONS = (names.AVERAGE_CPV, names.AVERAGE_CPM, names.CONTRACTED_RATE, names.COST_SHARE, names.CTR_I,
                names.CTR_V, names.IMPRESSIONS_SHARE, names.MARGIN, names.PROFIT, names.REVENUE,
                names.VIDEO_VIEWS_SHARE, names.VIDEO_VIEW_RATE, names.SUM_IMPRESSIONS, names.SUM_VIDEO_VIEWS,
                names.SUM_CLICKS, names.SUM_COST)


class CreateAccountTargetingReportTargetDataTestCase(TransactionTestCase):
    def _create_salesforce(self, pl_params=None):
        op = Opportunity.objects.create(id=f"OPTEST{next(int_iterator)}",
                                        name=f"test_op{next(int_iterator)}")
        params = dict(
            opportunity=op,

        )
        params.update(pl_params or {})
        pl = OpPlacement.objects.create(**params)
        return pl

    def _assert_statistics(self, targeting_data, stats, campaign, placement, targeting_obj):
        sum_impressions = sum(item.impressions for item in stats)
        sum_video_views = sum(item.video_views for item in stats)
        sum_cost = sum(item.cost for item in stats)
        sum_clicks = sum(item.clicks for item in stats)
        if placement.goal_type_id == 0:
            revenue = sum_impressions * placement.ordered_rate / 1000
        else:
            revenue = sum_video_views * placement.ordered_rate
        impressions_share = sum_impressions / campaign.impressions
        video_views_share = sum_video_views / campaign.video_views
        profit = revenue - sum_cost
        margin = (revenue - sum_cost) / revenue
        ctr_i = sum_clicks / sum_impressions
        ctr_v = sum_clicks / sum_video_views
        average_cpm = sum_cost / sum_impressions * 1000
        average_cpv = sum_cost / sum_video_views
        targeting_status = TargetingStatusEnum(targeting_obj.status).name

        self.assertEqual(targeting_data["sum_impressions"], sum_impressions)
        self.assertEqual(targeting_data["sum_video_views"], sum_video_views)
        self.assertEqual(targeting_data["impressions_share"], impressions_share)
        self.assertEqual(targeting_data["video_views_share"], video_views_share)
        self.assertEqual(targeting_data["contracted_rate"], placement.ordered_rate)
        self.assertAlmostEqual(targeting_data["sum_cost"], sum_cost, places=2)
        self.assertAlmostEqual(targeting_data["revenue"], revenue, places=4)
        self.assertAlmostEqual(targeting_data["profit"], profit, places=4)
        self.assertAlmostEqual(targeting_data["margin"], margin, places=4)
        self.assertAlmostEqual(targeting_data["ctr_i"], ctr_i, places=4)
        self.assertAlmostEqual(targeting_data["ctr_v"], ctr_v, places=4)
        self.assertAlmostEqual(targeting_data["average_cpm"], average_cpm, places=4)
        self.assertAlmostEqual(targeting_data["average_cpv"], average_cpv, places=4)
        self.assertEqual(targeting_data["targeting_status"], targeting_status)

    def test_age_range(self):
        account = Account.objects.create()
        pl_params = dict(
            ordered_rate=1.5,
            goal_type_id=0,  # CPM
        )
        pl = self._create_salesforce(pl_params)
        campaign = Campaign.objects.create(account=account, salesforce_placement=pl, impressions=430, video_views=118)
        ag_18_24 = AdGroup.objects.create(name="18_24", campaign=campaign)
        ag_25_34 = AdGroup.objects.create(name="25_34", campaign=campaign)
        criteria = CriteriaType.objects.create(id=CriteriaTypeEnum.AGE_RANGE.value,
                                               name=CriteriaTypeEnum.AGE_RANGE.name)
        targeting_18_24 = AdGroupTargeting.objects.create(
            ad_group=ag_18_24,
            status=TargetingStatusEnum.ENABLED.value,
            type=criteria,
            statistic_criteria=AgeRange.AGE_18_24,
        )
        targeting_25_34 = AdGroupTargeting.objects.create(
            ad_group=ag_25_34,
            status=TargetingStatusEnum.ENABLED.value,
            type=criteria,
            statistic_criteria=AgeRange.AGE_25_34,
        )
        stat_1 = AgeRangeStatistic.objects.create(ad_group=ag_18_24, age_range_id=AgeRange.AGE_18_24,
                                                  date=date.today(),
                                                  impressions=55, video_views=43, cost=3.1, clicks=4)
        stat_2 = AgeRangeStatistic.objects.create(ad_group=ag_18_24, age_range_id=AgeRange.AGE_18_24,
                                                  date=date.today() + timedelta(days=1),
                                                  impressions=34, video_views=12, cost=5.82, clicks=12)

        stat_3 = AgeRangeStatistic.objects.create(ad_group=ag_25_34, age_range_id=AgeRange.AGE_25_34,
                                                  date=date.today(),
                                                  impressions=13, video_views=11, cost=1.8, clicks=3)
        stat_4 = AgeRangeStatistic.objects.create(ad_group=ag_25_34, age_range_id=AgeRange.AGE_25_34,
                                                  date=date.today() + timedelta(days=1),
                                                  impressions=16, video_views=33, cost=2.19, clicks=5)
        report = AccountTargetingReport(account, criterion_types=CriteriaTypeEnum.AGE_RANGE.name)
        report.prepare_report(aggregation_columns=AGGREGATIONS)
        targeting_data = report.get_targeting_report(sort_key="criteria")
        self._assert_statistics(targeting_data[0], [stat_1, stat_2], campaign, pl, targeting_18_24)
        self._assert_statistics(targeting_data[1], [stat_3, stat_4], campaign, pl, targeting_25_34)

    def test_parents(self):
        account = Account.objects.create()
        pl_params = dict(
            ordered_rate=0.85,
            goal_type_id=1,  # CPV
        )
        pl = self._create_salesforce(pl_params)
        campaign = Campaign.objects.create(account=account, salesforce_placement=pl, impressions=765, video_views=243)
        ag_parent = AdGroup.objects.create(name="parent", campaign=campaign)
        ag_not_parent = AdGroup.objects.create(name="not_parent", campaign=campaign)
        criteria = CriteriaType.objects.create(id=CriteriaTypeEnum.PARENT.value, name=CriteriaTypeEnum.PARENT.name)
        targeting_parent = AdGroupTargeting.objects.create(
            ad_group=ag_parent,
            status=TargetingStatusEnum.PAUSED.value,
            type=criteria,
            statistic_criteria=Parent.PARENT,
        )
        targeting_not_parent = AdGroupTargeting.objects.create(
            ad_group=ag_not_parent,
            status=TargetingStatusEnum.ENABLED.value,
            type=criteria,
            statistic_criteria=Parent.NOT_A_PARENT,
        )
        stat_1 = ParentStatistic.objects.create(ad_group=ag_parent, parent_status_id=Parent.PARENT, date=date.today(),
                                                impressions=28, video_views=13, cost=2.9, clicks=2)
        stat_2 = ParentStatistic.objects.create(ad_group=ag_parent, parent_status_id=Parent.PARENT,
                                                date=date.today() + timedelta(days=1),
                                                impressions=54, video_views=5, cost=4.6, clicks=5)

        stat_3 = ParentStatistic.objects.create(ad_group=ag_not_parent, parent_status_id=Parent.NOT_A_PARENT,
                                                date=date.today(),
                                                impressions=11, video_views=17, cost=2.8, clicks=1)
        stat_4 = ParentStatistic.objects.create(ad_group=ag_not_parent, parent_status_id=Parent.NOT_A_PARENT,
                                                date=date.today() + timedelta(days=1),
                                                impressions=46, video_views=13, cost=1.2, clicks=0)
        report = AccountTargetingReport(account, criterion_types=CriteriaTypeEnum.PARENT.name)
        report.prepare_report(aggregation_columns=AGGREGATIONS)
        targeting_data = report.get_targeting_report(sort_key="criteria")
        self._assert_statistics(targeting_data[0], [stat_1, stat_2], campaign, pl, targeting_parent)
        self._assert_statistics(targeting_data[1], [stat_3, stat_4], campaign, pl, targeting_not_parent)

    def test_gender(self):
        account = Account.objects.create()
        pl_params = dict(
            ordered_rate=1.78,
            goal_type_id=0,
        )
        pl = self._create_salesforce(pl_params)
        campaign = Campaign.objects.create(account=account, salesforce_placement=pl, impressions=1290, video_views=433)
        ag_male = AdGroup.objects.create(name="male", campaign=campaign)
        ag_female = AdGroup.objects.create(name="female", campaign=campaign)
        criteria = CriteriaType.objects.create(id=CriteriaTypeEnum.GENDER.value, name=CriteriaTypeEnum.GENDER.name)
        targeting_male = AdGroupTargeting.objects.create(
            ad_group=ag_male,
            status=TargetingStatusEnum.ENABLED.value,
            type=criteria,
            statistic_criteria=Gender.MALE,
        )
        targeting_female = AdGroupTargeting.objects.create(
            ad_group=ag_female,
            status=TargetingStatusEnum.PAUSED.value,
            type=criteria,
            statistic_criteria=Gender.FEMALE,
        )
        stat_1 = GenderStatistic.objects.create(ad_group=ag_male, gender_id=Gender.MALE, date=date.today(),
                                                impressions=152, video_views=42, cost=7.1, clicks=7)
        stat_2 = GenderStatistic.objects.create(ad_group=ag_male, gender_id=Gender.MALE,
                                                date=date.today() + timedelta(days=1),
                                                impressions=54, video_views=5, cost=4.6, clicks=5)

        stat_3 = GenderStatistic.objects.create(ad_group=ag_female, gender_id=Gender.FEMALE, date=date.today(),
                                                impressions=220, video_views=75, cost=10.4, clicks=0)
        stat_4 = GenderStatistic.objects.create(ad_group=ag_female, gender_id=Gender.FEMALE,
                                                date=date.today() + timedelta(days=1),
                                                impressions=332, video_views=101, cost=12.8, clicks=0)
        report = AccountTargetingReport(account, criterion_types=CriteriaTypeEnum.GENDER.name)
        report.prepare_report(aggregation_columns=AGGREGATIONS)
        targeting_data = report.get_targeting_report(sort_key="criteria")
        targeting_data.sort(key=lambda x: x["ad_group_name"])
        self._assert_statistics(targeting_data[0], [stat_3, stat_4], campaign, pl, targeting_female)
        self._assert_statistics(targeting_data[1], [stat_1, stat_2], campaign, pl, targeting_male)

    def test_topics(self):
        account = Account.objects.create()
        pl_params = dict(
            ordered_rate=1.2,
            goal_type_id=1,  # CPV
        )
        pl = self._create_salesforce(pl_params)
        campaign = Campaign.objects.create(account=account, salesforce_placement=pl, impressions=912, video_views=54)
        ad_group = AdGroup.objects.create(name="topics", campaign=campaign)
        topic, _ = Topic.objects.get_or_create(name=f"Test topic {next(int_iterator)}")
        criteria = CriteriaType.objects.create(id=CriteriaTypeEnum.VERTICAL.value, name=CriteriaTypeEnum.VERTICAL.name)
        targeting = AdGroupTargeting.objects.create(
            ad_group=ad_group,
            status=TargetingStatusEnum.ENABLED.value,
            type=criteria,
            statistic_criteria=topic.id,
        )
        stat_1 = TopicStatistic.objects.create(ad_group=ad_group, topic=topic, date=date.today(), impressions=5,
                                               video_views=9, cost=1.3, clicks=2)
        stat_2 = TopicStatistic.objects.create(ad_group=ad_group, topic=topic, date=date.today() + timedelta(days=1),
                                               impressions=16, video_views=33, cost=2.19, clicks=5)
        report = AccountTargetingReport(account, criterion_types=CriteriaTypeEnum.VERTICAL.name)
        report.prepare_report(
            aggregation_columns=AGGREGATIONS
        )
        targeting_data = report.get_targeting_report()
        self._assert_statistics(targeting_data[0], [stat_1, stat_2], campaign, pl, targeting)

    def test_keyword(self):
        account = Account.objects.create()
        pl_params = dict(
            ordered_rate=3.5,
            goal_type_id=0,
        )
        pl = self._create_salesforce(pl_params)
        campaign = Campaign.objects.create(account=account, salesforce_placement=pl, impressions=9082, video_views=542)
        ag_1 = AdGroup.objects.create(name=f"keywords{next(int_iterator)}", campaign=campaign)
        ag_2 = AdGroup.objects.create(name=f"keywords{next(int_iterator)}", campaign=campaign)
        keyword_1 = f"test_keyword{next(int_iterator)}"
        keyword_2 = f"another_test_keyword{next(int_iterator)}"
        criteria = CriteriaType.objects.create(id=CriteriaTypeEnum.KEYWORD.value, name=CriteriaTypeEnum.KEYWORD.name)
        targeting_1 = AdGroupTargeting.objects.create(
            ad_group=ag_1,
            status=TargetingStatusEnum.ENABLED.value,
            type=criteria,
            statistic_criteria=keyword_1,
        )
        targeting_2 = AdGroupTargeting.objects.create(
            ad_group=ag_2,
            status=TargetingStatusEnum.PAUSED.value,
            type=criteria,
            statistic_criteria=keyword_2,
        )
        stat_1 = KeywordStatistic.objects.create(ad_group=ag_1, keyword=keyword_1, date=date.today(),
                                                 impressions=643, video_views=123, cost=11.7, clicks=5)
        stat_2 = KeywordStatistic.objects.create(ad_group=ag_1, keyword=keyword_1,
                                                 date=date.today() + timedelta(days=1),
                                                 impressions=559, video_views=73, cost=10.8, clicks=11)

        stat_3 = KeywordStatistic.objects.create(ad_group=ag_2, keyword=keyword_2,
                                                 date=date.today(),
                                                 impressions=872, video_views=113, cost=12.6, clicks=25)
        stat_4 = KeywordStatistic.objects.create(ad_group=ag_2, keyword=keyword_2,
                                                 date=date.today() + timedelta(days=1),
                                                 impressions=87, video_views=4, cost=0.98, clicks=5)
        report = AccountTargetingReport(account, criterion_types=CriteriaTypeEnum.KEYWORD.name)
        report.prepare_report(aggregation_columns=AGGREGATIONS)
        targeting_data = report.get_targeting_report(sort_key="criteria")
        targeting_data.sort(key=lambda x: x["ad_group_name"])
        self._assert_statistics(targeting_data[0], [stat_1, stat_2], campaign, pl, targeting_1)
        self._assert_statistics(targeting_data[1], [stat_3, stat_4], campaign, pl, targeting_2)

    def test_audience(self):
        account = Account.objects.create()
        pl_params = dict(
            ordered_rate=1.2,
            goal_type_id=1,
        )
        pl = self._create_salesforce(pl_params)
        campaign = Campaign.objects.create(account=account, salesforce_placement=pl, impressions=10082,
                                           video_views=776)
        audience_1 = Audience.objects.create(name=f"test_audience{next(int_iterator)}")
        audience_2 = Audience.objects.create(name=f"test_audience{next(int_iterator)}")
        audience_remarketing = RemarkList.objects.create(name=f"test_remarketing{next(int_iterator)}")
        ag_1 = AdGroup.objects.create(name=f"audience{next(int_iterator)}", campaign=campaign)
        ag_2 = AdGroup.objects.create(name=f"audience{next(int_iterator)}", campaign=campaign)
        ag_remarketing = AdGroup.objects.create(name=f"remarketing{next(int_iterator)}", campaign=campaign)
        criteria_interest = CriteriaType.objects.create(id=CriteriaTypeEnum.USER_INTEREST.value,
                                                        name=CriteriaTypeEnum.USER_INTEREST.name)
        criteria_list = CriteriaType.objects.create(id=CriteriaTypeEnum.USER_LIST.value,
                                                    name=CriteriaTypeEnum.USER_LIST.name)
        targeting_1 = AdGroupTargeting.objects.create(
            ad_group=ag_1,
            status=TargetingStatusEnum.ENABLED.value,
            type=criteria_interest,
            statistic_criteria=audience_1.id,
        )
        targeting_2 = AdGroupTargeting.objects.create(
            ad_group=ag_2,
            status=TargetingStatusEnum.PAUSED.value,
            type=criteria_interest,
            statistic_criteria=audience_2.id,
        )
        targeting_remarketing = AdGroupTargeting.objects.create(
            ad_group=ag_remarketing,
            status=TargetingStatusEnum.PAUSED.value,
            type=criteria_list,
            statistic_criteria=audience_remarketing.id,
        )
        stat_1 = AudienceStatistic.objects.create(ad_group=ag_1, audience_id=audience_1.id, date=date.today(),
                                                  impressions=643, video_views=123, cost=11.7, clicks=5)
        stat_2 = AudienceStatistic.objects.create(ad_group=ag_1, audience_id=audience_1.id,
                                                  date=date.today() + timedelta(days=1),
                                                  impressions=559, video_views=73, cost=10.8, clicks=11)

        stat_3 = AudienceStatistic.objects.create(ad_group=ag_2, audience_id=audience_2.id, date=date.today(),
                                                  impressions=672, video_views=161, cost=11.6, clicks=15)
        stat_4 = AudienceStatistic.objects.create(ad_group=ag_2, audience_id=audience_2.id,
                                                  date=date.today() + timedelta(days=1),
                                                  impressions=387, video_views=45, cost=1.98, clicks=12)

        stat_5 = RemarkStatistic.objects.create(ad_group=ag_remarketing, remark_id=audience_remarketing.id,
                                                date=date.today(), impressions=872, video_views=113, cost=12.6,
                                                clicks=25)
        stat_6 = RemarkStatistic.objects.create(ad_group=ag_remarketing, remark_id=audience_remarketing.id,
                                                date=date.today() + timedelta(days=1), impressions=87, video_views=4,
                                                cost=0.98, clicks=5)
        report = AccountTargetingReport(account, criterion_types=[CriteriaTypeEnum.USER_INTEREST.name,
                                                                  CriteriaTypeEnum.USER_LIST.name])
        report.prepare_report(aggregation_columns=AGGREGATIONS)
        targeting_data = report.get_targeting_report(sort_key="target_name")
        self._assert_statistics(targeting_data[0], [stat_1, stat_2], campaign, pl, targeting_1)
        self._assert_statistics(targeting_data[1], [stat_3, stat_4], campaign, pl, targeting_2)
        self._assert_statistics(targeting_data[2], [stat_5, stat_6], campaign, pl, targeting_remarketing)

    def test_placement(self):
        account = Account.objects.create()
        pl_params = dict(
            ordered_rate=1.78,
            goal_type_id=0,
        )
        pl = self._create_salesforce(pl_params)
        campaign = Campaign.objects.create(account=account, salesforce_placement=pl, impressions=9290,
                                           video_views=1943)
        ag_channel = AdGroup.objects.create(name="channel", campaign=campaign)
        ag_video = AdGroup.objects.create(name="video", campaign=campaign)
        criteria = CriteriaType.objects.create(id=CriteriaTypeEnum.PLACEMENT.value,
                                               name=CriteriaTypeEnum.PLACEMENT.name)
        yt_channel_id = f"test_channel_id{next(int_iterator)}"
        yt_video_id = f"test_video_id{next(int_iterator)}"
        targeting_channel = AdGroupTargeting.objects.create(
            ad_group=ag_channel,
            status=TargetingStatusEnum.ENABLED.value,
            type=criteria,
            statistic_criteria=yt_channel_id,
        )
        targeting_video = AdGroupTargeting.objects.create(
            ad_group=ag_video,
            status=TargetingStatusEnum.PAUSED.value,
            type=criteria,
            statistic_criteria=yt_video_id,
        )
        stat_1 = YTChannelStatistic.objects.create(ad_group=ag_channel, yt_id=yt_channel_id, date=date.today(),
                                                   impressions=152, video_views=42, cost=7.1, clicks=7)
        stat_2 = YTChannelStatistic.objects.create(ad_group=ag_channel, yt_id=yt_channel_id,
                                                   date=date.today() + timedelta(days=1),
                                                   impressions=54, video_views=5, cost=4.6, clicks=5)

        stat_3 = YTVideoStatistic.objects.create(ad_group=ag_video, yt_id=yt_video_id, date=date.today(),
                                                 impressions=220, video_views=75, cost=10.4, clicks=0)
        stat_4 = YTVideoStatistic.objects.create(ad_group=ag_video, yt_id=yt_video_id,
                                                 date=date.today() + timedelta(days=1),
                                                 impressions=332, video_views=101, cost=12.8, clicks=0)
        criteria_types = [f"{CriteriaTypeEnum.PLACEMENT.name}_CHANNEL", f"{CriteriaTypeEnum.PLACEMENT.name}_VIDEO"]
        report = AccountTargetingReport(account, criterion_types=criteria_types)
        report.prepare_report(aggregation_columns=AGGREGATIONS)
        targeting_data = report.get_targeting_report(sort_key="criteria")
        self._assert_statistics(targeting_data[0], [stat_1, stat_2], campaign, pl, targeting_channel)
        self._assert_statistics(targeting_data[1], [stat_3, stat_4], campaign, pl, targeting_video)

    def test_percentage_annotation_100_max(self):
        """ Ensure certain percentages do not exceed 100% """
        account = Account.objects.create()
        pl_params = dict(
            ordered_rate=1.2,
            goal_type_id=1,  # CPV
        )
        pl = self._create_salesforce(pl_params)
        campaign = Campaign.objects.create(account=account, salesforce_placement=pl, impressions=100, video_views=100,
                                           cost=100)
        ad_group = AdGroup.objects.create(name="topics", campaign=campaign)
        topic, _ = Topic.objects.get_or_create(name=f"Test topic {next(int_iterator)}")
        criteria = CriteriaType.objects.create(id=CriteriaTypeEnum.VERTICAL.value, name=CriteriaTypeEnum.VERTICAL.name)
        AdGroupTargeting.objects.create(
            ad_group=ad_group,
            status=TargetingStatusEnum.ENABLED.value,
            type=criteria,
            statistic_criteria=topic.id,
        )
        stat_1 = TopicStatistic.objects.create(ad_group=ad_group, topic=topic, date=date.today(), impressions=50,
                                               video_views=50, cost=50)
        stat_2 = TopicStatistic.objects.create(ad_group=ad_group, topic=topic, date=date.today() + timedelta(days=1),
                                               impressions=51, video_views=51, cost=51)

        self.assertTrue((stat_1.impressions + stat_2.impressions) / campaign.impressions > 1.0)
        self.assertTrue((stat_1.video_views + stat_2.video_views) / campaign.video_views > 1.0)
        self.assertTrue((stat_1.cost + stat_2.cost) / campaign.cost > 1.0)

        report = AccountTargetingReport(account, criterion_types=CriteriaTypeEnum.VERTICAL.name)
        report.prepare_report(
            aggregation_columns=AGGREGATIONS
        )
        targeting_data = report.get_targeting_report()[0]
        self.assertEqual(targeting_data["impressions_share"], 1.0)
        self.assertEqual(targeting_data["video_views_share"], 1.0)
        self.assertEqual(targeting_data["cost_share"], 1.0)
        self.assertEqual(targeting_data["video_view_rate"], 1.0)
