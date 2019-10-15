from datetime import date
from datetime import timedelta
from unittest.case import skip

from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import device_str
from utils.utittests.patch_now import patch_now
from .base import ColumnsDeclaration
from .base import CreateOpportunityTargetingReportSheetTestCase


class CreateOpportunityTargetingReportDevicesDataTestCase(CreateOpportunityTargetingReportSheetTestCase):
    SHEET_NAME = "Devices"

    columns = ColumnsDeclaration(
        (
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
        device_id = 1
        AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=device_id, date=any_date, average_position=1)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(device_str(device_id), item[columns.type])
        self.assertEqual(self.campaign.name, item[columns.campaign_name])
        self.assertEqual(self.ad_group.name, item[columns.ad_group_name])
        self.assertEqual(self.placement.name, item[columns.placement_name])
        self.assertEqual(str(self.placement.start), item[columns.placement_start])
        self.assertEqual(str(self.placement.end), item[columns.placement_end])
        self.assertEqual(self.opportunity.cannot_roll_over, item[columns.cannot_roll_over])
        self.assertEqual(self.placement.goal_type, item[columns.goal_type])
        self.assertEqual(self.placement.ordered_rate, item[columns.contracted_rate])

    def test_general_stats(self):
        any_date = date(2019, 1, 1)
        stats = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
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
        AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1)

        with patch_now(test_now):
            self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(days_remaining, item[columns.days_remaining])

    def test_margin_cap(self):
        any_date = date(2019, 1, 1)
        AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
                                        impressions=1000, video_views=200, cost=1.02, clicks=30)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(self.opportunity.margin_cap_required, item[columns.margin_cap])

    @skip("Not implemented")
    def test_max_bid(self):
        raise NotImplementedError

    def test_average_rate_cpv(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPV
        self.placement.save()
        stats = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
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
        stats = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
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
        costs = (20, 80)

        stats_1 = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date,
                                                  average_position=1,
                                                  cost=costs[0])
        stats_2 = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=2, date=any_date,
                                                  average_position=1,
                                                  cost=costs[1])

        device_1 = device_str(stats_1.device_id)
        device_2 = device_str(stats_2.device_id)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(2, len(data))
        columns = self.columns
        topics_cost_delivery_percentage = {
            item[columns.type]: item[columns.cost_delivered_percentage]
            for item in data
        }
        sum_cost = sum(costs)

        self.assertEqual(
            stats_1.cost / sum_cost,
            topics_cost_delivery_percentage[device_1]
        )
        self.assertEqual(
            stats_2.cost / sum_cost,
            topics_cost_delivery_percentage[device_2]
        )

    def test_delivery_percentage_cpm(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPM
        self.placement.save()
        impressions = (20, 80)
        stats_1 = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date,
                                                  average_position=1,
                                                  impressions=impressions[0])
        stats_2 = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=2, date=any_date,
                                                  average_position=1,
                                                  impressions=impressions[1])

        device_1 = device_str(stats_1.device_id)
        device_2 = device_str(stats_2.device_id)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(2, len(data))
        columns = self.columns
        topics_cost_delivery_percentage = {
            item[columns.type]: item[columns.delivery_percentage]
            for item in data
        }
        sum_impressions = sum(impressions)
        self.assertEqual(
            stats_1.impressions / sum_impressions,
            topics_cost_delivery_percentage[device_1]
        )
        self.assertEqual(
            stats_2.impressions / sum_impressions,
            topics_cost_delivery_percentage[device_2]
        )

    def test_delivery_percentage_cpv(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPV
        self.placement.save()

        views = (20, 80)
        stats_1 = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date,
                                                  average_position=1,
                                                  video_views=views[0])
        stats_2 = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=2, date=any_date,
                                                  average_position=1,
                                                  video_views=views[1])

        device_1 = device_str(stats_1.device_id)
        device_2 = device_str(stats_2.device_id)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(2, len(data))
        columns = self.columns
        topics_cost_delivery_percentage = {
            item[columns.type]: item[columns.delivery_percentage]
            for item in data
        }
        sum_views = sum(views)

        self.assertEqual(
            stats_1.video_views / sum_views,
            topics_cost_delivery_percentage[device_1]
        )
        self.assertEqual(
            stats_2.video_views / sum_views,
            topics_cost_delivery_percentage[device_2]
        )

    def test_revenue_cpv(self):
        any_date = date(2019, 1, 1)
        self.placement.goal_type_id = SalesForceGoalType.CPV
        self.placement.save()
        stats = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
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
        stats = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
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
        stats = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
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
        stats = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
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
        stats = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
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
        stats = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
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
        AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
                                        impressions=200, clicks=30)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(None, item[columns.view_rate])

    def test_ctr(self):
        any_date = date(2019, 1, 1)
        stats = AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
                                                impressions=200, clicks=30)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertAlmostEqual(stats.clicks / stats.impressions, item[columns.ctr])

    def test_ordering(self):
        any_date = date(2019, 1, 1)

        AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, date=any_date, average_position=1,
                                        video_views=1)
        AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=3, date=any_date, average_position=1,
                                        video_views=2)
        AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=2, date=any_date, average_position=1,
                                        video_views=3)

        self.act(self.opportunity.id, any_date, any_date)
        data = self.get_data_dict(self.opportunity.id, any_date, any_date)
        views = [item[self.columns.views] for item in data]
        self.assertEqual(
            sorted(views, reverse=True),
            views
        )

    def test_general_stats_aggregates(self):
        any_date = date(2019, 1, 1)
        impressions = (1000, 2100)
        period = len(impressions)
        date_from, date_to = any_date, any_date + timedelta(days=period)
        for index in range(period):
            AdGroupStatistic.objects.create(ad_group=self.ad_group, device_id=1, average_position=1,
                                            date=any_date + timedelta(days=index),
                                            impressions=impressions[index])

        self.act(self.opportunity.id, date_from, date_to)
        data = self.get_data_dict(self.opportunity.id, date_from, date_to)
        self.assertEqual(1, len(data))
        item = data[0]
        columns = self.columns
        self.assertEqual(sum(impressions), item[columns.impressions])
