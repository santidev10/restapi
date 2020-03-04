from datetime import date
from datetime import timedelta
from unittest import skip

from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from utils.unittests.patch_now import patch_now
from utils.unittests.str_iterator import str_iterator
from .base import ColumnsDeclaration
from .base import CreateOpportunityTargetingReportSheetTestCase


class CreateOpportunityTargetingReportTargetTestCase(CreateOpportunityTargetingReportSheetTestCase):
    SHEET_NAME = "Target"

    columns = ColumnsDeclaration(
        (
            ("target", "Target"),
            ("type", "Type"),
            ("campaign_name", "Ads Campaign"),
            ("ad_group_name", "Ads Ad group"),
            ("placement_name", "Salesforce Placement"),
            ("placement_start", "Placement Start Date"),
            ("placement_end", "Placement End Date"),
            ("days_remaining", "Days remaining"),
            ("margin_cap", "Margin Cap"),
            ("cannot_roll_over", "Cannot Roll over Delivery"),
            ("goal_type", "Rate Type"),
            ("contracted_rate", "Contracted Rate"),
            ("max_bid", "Max bid"),
            ("avg_rate", "Avg. Rate"),
            ("cost", "Cost"),
            ("cost_delivered_percentage", "Cost delivery percentage"),
            ("impressions", "Impressions"),
            ("views", "Views"),
            ("delivery_percentage", "Delivery percentage"),
            ("revenue", "Revenue"),
            ("profit", "Profit"),
            ("margin", "Margin"),
            ("video_played_100", "Video played to 100%"),
            ("view_rate", "View rate"),
            ("clicks", "Clicks"),
            ("ctr", "CTR"),
        )
    )


class CreateOpportunityTargetingReportTargetDataTestCase(CreateOpportunityTargetingReportTargetTestCase):
    def get_data_dict(self, *args, **kwargs):
        rows = self.get_data_table(*args, **kwargs)
        headers = [cell.value for cell in rows[0]]

        return [
            dict(zip(headers, [cell.value for cell in row]))
            for row in rows[1:]
        ]

    def test_headers(self):
        any_date = date(2019, 1, 1)
        self.act(self.opportunity.id, any_date, any_date)

        rows = self.get_data_table(self.opportunity.id, any_date, any_date)
        headers = rows[0]
        self.assertEqual(
            list(self.columns.labels()),
            [cell.value for cell in headers]
        )

    def test_general_data(self):
        self.opportunity.cannot_roll_over = True
        self.opportunity.save()
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(self.campaign.name, item[columns.campaign_name])
        self.assertEqual(self.ad_group.name, item[columns.ad_group_name])
        self.assertEqual(self.placement.name, item[columns.placement_name])
        self.assertEqual(str(self.placement.start), item[columns.placement_start])
        self.assertEqual(str(self.placement.end), item[columns.placement_end])
        self.assertEqual(self.opportunity.cannot_roll_over, item[columns.cannot_roll_over])
        self.assertEqual(self.placement.goal_type, item[columns.goal_type])
        self.assertEqual(self.placement.ordered_rate, item[columns.contracted_rate])

    def test_topic_general_data(self):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(topic.name, item[columns.target])
        self.assertEqual("Topic", item[columns.type])

    def test_keyword_general_data(self):
        any_date = date(2019, 1, 1)
        self.opportunity.cannot_roll_over = True
        self.opportunity.save()
        keyword = "test keyword"
        KeywordStatistic.objects.create(keyword=keyword, ad_group=self.ad_group, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(keyword, item[columns.target])
        self.assertEqual("Keyword", item[columns.type])

    def test_interests_in_marketing_general_data(self):
        any_date = date(2019, 1, 1)
        self.opportunity.cannot_roll_over = True
        self.opportunity.save()

        audience_name = "Test Audience"
        audience = Audience.objects.create(type=Audience.IN_MARKET_TYPE, name=audience_name)
        AudienceStatistic.objects.create(audience=audience, ad_group=self.ad_group, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(audience_name, item[columns.target])
        self.assertEqual("Interests - In market", item[columns.type])

    def test_interests_affinity_general_data(self):
        any_date = date(2019, 1, 1)
        self.opportunity.cannot_roll_over = True
        self.opportunity.save()

        audience_name = "Test Audience"
        audience = Audience.objects.create(type=Audience.AFFINITY_TYPE, name=audience_name)
        AudienceStatistic.objects.create(audience=audience, ad_group=self.ad_group, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(audience_name, item[columns.target])
        self.assertEqual("Interests - Affinity", item[columns.type])

    def test_interests_caa_general_data(self):
        any_date = date(2019, 1, 1)
        self.opportunity.cannot_roll_over = True
        self.opportunity.save()

        audience_name = "Test Audience"
        audience = Audience.objects.create(type=Audience.CUSTOM_AFFINITY_TYPE, name=audience_name)
        AudienceStatistic.objects.create(audience=audience, ad_group=self.ad_group, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(audience_name, item[columns.target])
        self.assertEqual("Interests - CAA", item[columns.type])

    def test_interests_custom_intent_general_data(self):
        any_date = date(2019, 1, 1)
        self.opportunity.cannot_roll_over = True
        self.opportunity.save()

        audience_name = "Test Audience"
        audience = Audience.objects.create(type=Audience.CUSTOM_INTENT_TYPE, name=audience_name)
        AudienceStatistic.objects.create(audience=audience, ad_group=self.ad_group, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(audience_name, item[columns.target])
        self.assertEqual("Interests - Custom Intent", item[columns.type])

    @skip("Not implemented")
    def test_interests_detailed_demographic_general_data(self):
        raise NotImplemented

    def test_general_stats(self):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        stats = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                              impressions=1000, video_views=200, cost=1.02, clicks=30)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(stats.impressions, item[columns.impressions])
        self.assertEqual(stats.video_views, item[columns.views])
        self.assertEqual(stats.cost, item[columns.cost])
        self.assertEqual(stats.clicks, item[columns.clicks])

    def test_days_remaining(self):
        any_date = date(2019, 1, 1)
        days_remaining = 3
        test_now = self.placement.end - timedelta(days=days_remaining)
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date)

        with patch_now(test_now):
            self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(days_remaining, item[columns.days_remaining])

    def test_margin_cap(self):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                      impressions=1000, video_views=200, cost=1.02, clicks=30)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(self.opportunity.margin_cap_required, item[columns.margin_cap])

    def test_max_bid(self):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                      impressions=1000, video_views=200, cost=1.02, clicks=30)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(self.ad_group.cpv_bid / 10 ** 6, item[columns.max_bid])

    def test_average_rate_cpv(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPV
        self.placement.save()
        topic = Topic.objects.create(name="Test topic")
        stats = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                              cost=23, video_views=34)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        expected_rate = stats.cost / stats.video_views
        self.assertAlmostEqual(expected_rate, item[columns.avg_rate])

    def test_average_rate_cpm(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPM
        self.placement.save()
        topic = Topic.objects.create(name="Test topic")
        stats = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                              cost=23, impressions=34)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        expected_rate = stats.cost / (stats.impressions / 1000)
        self.assertAlmostEqual(expected_rate, item[columns.avg_rate])

    def test_cost_delivery_percentage(self):
        any_date = date(2019, 1, 1)
        topic_1 = Topic.objects.create(name="Test topic 1")
        topic_2 = Topic.objects.create(name="Test topic 2")
        costs = (20, 80)
        stats_1 = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic_1,
                                                date=any_date,
                                                cost=costs[0])
        stats_2 = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic_2,
                                                date=any_date,
                                                cost=costs[1])

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(2, len(data))
        columns = self.columns
        topics_cost_delivery_percentage = {
            item[columns.target]: item[columns.cost_delivered_percentage]
            for item in data
        }
        sum_cost = sum(costs)
        self.assertEqual(
            stats_1.cost / sum_cost,
            topics_cost_delivery_percentage[topic_1.name]
        )
        self.assertEqual(
            stats_2.cost / sum_cost,
            topics_cost_delivery_percentage[topic_2.name]
        )

    def test_cost_delivery_percentage_by_interests(self):
        any_date = date(2019, 1, 1)
        costs = (20, 80)

        audience1 = Audience.objects.create(type=Audience.IN_MARKET_TYPE, name="Test Audience 1")
        audience2 = Audience.objects.create(type=Audience.IN_MARKET_TYPE, name="Test Audience 2")

        stats_1 = AudienceStatistic.objects.create(audience=audience1, ad_group=self.ad_group, date=any_date,
                                                   cost=costs[0])
        stats_2 = AudienceStatistic.objects.create(audience=audience2, ad_group=self.ad_group, date=any_date,
                                                   cost=costs[1])

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(2, len(data))
        columns = self.columns
        topics_cost_delivery_percentage = {
            item[columns.target]: item[columns.cost_delivered_percentage]
            for item in data
        }
        sum_cost = sum(costs)
        self.assertEqual(
            stats_1.cost / sum_cost,
            topics_cost_delivery_percentage[audience1.name]
        )
        self.assertEqual(
            stats_2.cost / sum_cost,
            topics_cost_delivery_percentage[audience2.name]
        )

    def test_delivery_percentage_cpm(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPM
        self.placement.save()
        topic_1 = Topic.objects.create(name="Test topic 1")
        topic_2 = Topic.objects.create(name="Test topic 2")
        impressions = (20, 80)
        stats_1 = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic_1,
                                                date=any_date,
                                                impressions=impressions[0])
        stats_2 = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic_2,
                                                date=any_date,
                                                impressions=impressions[1])

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(2, len(data))
        columns = self.columns
        topics_cost_delivery_percentage = {
            item[columns.target]: item[columns.delivery_percentage]
            for item in data
        }
        sum_impressions = sum(impressions)
        self.assertEqual(
            stats_1.impressions / sum_impressions,
            topics_cost_delivery_percentage[topic_1.name]
        )
        self.assertEqual(
            stats_2.impressions / sum_impressions,
            topics_cost_delivery_percentage[topic_2.name]
        )

    def test_delivery_percentage_cpv(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPV
        self.placement.save()
        topic_1 = Topic.objects.create(name="Test topic 1")
        topic_2 = Topic.objects.create(name="Test topic 2")
        views = (20, 80)
        stats_1 = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic_1,
                                                date=any_date,
                                                video_views=views[0])
        stats_2 = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic_2,
                                                date=any_date,
                                                video_views=views[1])

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(2, len(data))
        columns = self.columns
        topics_cost_delivery_percentage = {
            item[columns.target]: item[columns.delivery_percentage]
            for item in data
        }
        sum_views = sum(views)
        self.assertEqual(
            stats_1.video_views / sum_views,
            topics_cost_delivery_percentage[topic_1.name]
        )
        self.assertEqual(
            stats_2.video_views / sum_views,
            topics_cost_delivery_percentage[topic_2.name]
        )

    def test_revenue_cpv(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPV
        self.placement.save()
        topic = Topic.objects.create(name="Test topic")
        stats = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                              video_views=34)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertAlmostEqual(
            stats.video_views * self.placement.ordered_rate,
            item[columns.revenue]
        )

    def test_revenue_cpm(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPM
        self.placement.save()
        topic = Topic.objects.create(name="Test topic")
        stats = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                              impressions=3400)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertAlmostEqual(
            stats.impressions * self.placement.ordered_rate / 1000,
            item[columns.revenue]
        )

    def test_profit(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPV
        self.placement.save()
        topic = Topic.objects.create(name="Test topic")
        stats = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                              video_views=34, impressions=450, cost=13)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        revenue = stats.video_views * self.placement.ordered_rate
        expected_profit = revenue - stats.cost
        self.assertAlmostEqual(
            expected_profit,
            item[columns.profit]
        )

    def test_margin(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPV
        self.placement.save()
        topic = Topic.objects.create(name="Test topic")
        stats = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                              video_views=34, impressions=450, cost=13)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        revenue = stats.video_views * self.placement.ordered_rate
        profit = revenue - stats.cost
        expected_margin = profit / revenue
        self.assertAlmostEqual(
            expected_margin,
            item[columns.margin]
        )

    def test_video_played_to_100(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPV
        self.placement.save()
        topic = Topic.objects.create(name="Test topic")
        stats = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                              video_views_100_quartile=34.2, impressions=450)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        expected_video_played_100 = stats.video_views_100_quartile / stats.impressions
        self.assertAlmostEqual(
            expected_video_played_100,
            item[columns.video_played_100]
        )

    def test_view_rate_cpv(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPV
        self.placement.save()
        topic = Topic.objects.create(name="Test topic")
        stats = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                              impressions=200, video_views=30)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertAlmostEqual(stats.video_views / stats.impressions, item[columns.view_rate])

    def test_view_rate_cpm(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPM
        self.placement.save()
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                      impressions=200, clicks=30)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(None, item[columns.view_rate])

    def test_ctr(self):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        stats = TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date,
                                              impressions=200, clicks=30)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertAlmostEqual(stats.clicks / stats.impressions, item[columns.ctr])

    def test_ordering(self):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        keyword_1 = "keyword:1"
        keyword_2 = "keyword:2"
        KeywordStatistic.objects.create(keyword=keyword_1, ad_group=self.ad_group, date=any_date, video_views=1)
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date, video_views=2)
        KeywordStatistic.objects.create(keyword=keyword_2, ad_group=self.ad_group, date=any_date, video_views=3)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(3, len(data))
        targets = [item[self.columns.target] for item in data]
        self.assertEqual(
            [keyword_2, topic.name, keyword_1],
            targets
        )

    def test_general_stats_aggregates(self):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        impressions = (1000, 2100)
        period = len(impressions)
        date_from, date_to = any_date, any_date + timedelta(days=period)
        for index in range(period):
            TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic,
                                          date=any_date + timedelta(days=index),
                                          impressions=impressions[index])

        self.act(self.opportunity.id, date_from, date_to)
        data = self.get_data_dict(self.opportunity.id, date_from, date_to)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(sum(impressions), item[columns.impressions])

    def test_all_history(self):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date)

        self.act(self.opportunity.id, None, None)
        data = self.get_data_dict(self.opportunity.id, None, None)
        self.assertEqual(1, len(data))


class CreateOpportunityTargetingReportTargetDataESTestCase(CreateOpportunityTargetingReportTargetDataTestCase,
                                                           ESTestCase):
    def test_channel_general_data(self):
        any_date = date(2019, 1, 1)
        channel_id = next(str_iterator)
        YTChannelStatistic.objects.create(yt_id=channel_id, ad_group=self.ad_group, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(channel_id, item[columns.target])
        self.assertEqual("Channel", item[columns.type])

    def test_channel_general_data_title(self):
        channel_id = "test_channel:123"
        channel_name = "Test Channel Name"
        channel = Channel(id=channel_id)
        channel.populate_general_data(title=channel_name)
        ChannelManager(Sections.GENERAL_DATA).upsert([channel])
        any_date = date(2019, 1, 1)
        YTChannelStatistic.objects.create(yt_id=channel_id, ad_group=self.ad_group, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(channel_name, item[columns.target])

    def test_video_general_data(self):
        any_date = date(2019, 1, 1)
        video_id = next(str_iterator)
        YTVideoStatistic.objects.create(yt_id=video_id, ad_group=self.ad_group, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(video_id, item[columns.target])
        self.assertEqual("Video", item[columns.type])

    def test_video_general_data_title(self):
        any_date = date(2019, 1, 1)
        video_id = "test_channel:123"
        video_name = "Test Channel Name"
        video = Video(id=video_id)
        video.populate_general_data(title=video_name)
        VideoManager(Sections.GENERAL_DATA).upsert([video])
        YTVideoStatistic.objects.create(yt_id=video_id, ad_group=self.ad_group, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(video_name, item[columns.target])
