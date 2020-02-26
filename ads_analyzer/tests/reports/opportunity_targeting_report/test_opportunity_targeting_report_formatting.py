from datetime import date
from unittest.mock import patch

from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableTopicSerializer
from ads_analyzer.tests.reports.opportunity_targeting_report.test_opportunity_targeting_report_targeting import \
    CreateOpportunityTargetingReportTargetTestCase
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from utils.unittests.generic_test import generic_test


class CreateOpportunityTargetingReportTargetFormattingTestCase(CreateOpportunityTargetingReportTargetTestCase):
    class Color:
        RED = "FFFF0013"
        YELLOW = "FFFFFE50"
        GREEN = "FF00B25B"

    def get_cell_dict(self, *args, **kwargs):
        rows = self.get_data_table(*args, **kwargs)
        headers = [cell.value for cell in rows[0]]

        return [
            dict(zip(headers, row))
            for row in rows[1:]
        ]

    @generic_test([
        ("red", (.45, Color.RED), {}),
        ("yellow", (.55, Color.YELLOW), {}),
        ("green", (.75, Color.GREEN), {}),
        ("100%", (1., Color.GREEN), {}),
    ])
    def test_cost_delivery_percentage_background(self, value, color):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date)

        with patch.object(TargetTableTopicSerializer, "get_cost_delivery_percentage", return_value=value):
            self.act(self.opportunity.id, any_date, any_date)
        styles = self.get_cell_dict(self.opportunity.id, any_date, any_date)
        item = styles[0]
        columns = self.columns
        self.assertEqual(color, item[columns.cost_delivered_percentage].fill.start_color.rgb)

    @generic_test([
        ("red", (.45, Color.RED), {}),
        ("yellow", (.55, Color.YELLOW), {}),
        ("green", (.75, Color.GREEN), {}),
        ("100%", (1., Color.GREEN), {}),
    ])
    def test_delivery_percentage_background(self, value, color):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date)

        with patch.object(TargetTableTopicSerializer, "get_delivery_percentage", return_value=value):
            self.act(self.opportunity.id, any_date, any_date)
        styles = self.get_cell_dict(self.opportunity.id, any_date, any_date)
        item = styles[0]
        columns = self.columns
        self.assertEqual(color, item[columns.delivery_percentage].fill.start_color.rgb)

    @generic_test([
        ("red", (.25, Color.RED), {}),
        ("yellow", (.35, Color.YELLOW), {}),
        ("green", (.45, Color.GREEN), {}),
        ("100%", (1., Color.GREEN), {}),
        ("150%", (1.5, Color.GREEN), {}),
    ])
    def test_margin_background(self, value, color):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date)

        with patch.object(TargetTableTopicSerializer, "get_margin", return_value=value):
            self.act(self.opportunity.id, any_date, any_date)
        styles = self.get_cell_dict(self.opportunity.id, any_date, any_date)
        item = styles[0]
        columns = self.columns
        self.assertEqual(color, item[columns.margin].fill.start_color.rgb)

    def test_percentage_fields(self):
        any_date = date(2019, 1, 1)
        topic = Topic.objects.create(name="Test topic")
        TopicStatistic.objects.create(ad_group=self.ad_group, topic=topic, date=any_date)

        self.act(self.opportunity.id, any_date, any_date)
        styles = self.get_cell_dict(self.opportunity.id, any_date, any_date)
        item = styles[0]
        columns = self.columns
        self.assertEqual("0%", item[columns.margin].number_format)
        self.assertEqual("0.00%", item[columns.cost_delivered_percentage].number_format)
        self.assertEqual("0.00%", item[columns.delivery_percentage].number_format)
        self.assertEqual("0%", item[columns.video_played_100].number_format)
        self.assertEqual("0.00%", item[columns.view_rate].number_format)
        self.assertEqual("0.00%", item[columns.ctr].number_format)
