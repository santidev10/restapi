from aw_reporting.demo.charts import DemoChart
from aw_reporting.excel_reports import PerformanceWeeklyReport


class DemoAnalyzeWeeklyReport(PerformanceWeeklyReport):

    def get_campaign_data(self):
        return self.account.children

    def get_ad_group_data(self):
        return [a for c in self.account.children for a in c.children]

    def get_total_data(self):
        return self.account

    def get_interest_data(self):
        filters = dict(
            dimension="interest",
            start_date=self.date_delta,
        )
        charts_obj = DemoChart(self.account, filters)
        data = charts_obj.chart_items
        return data['items']

    def get_topic_data(self):
        filters = dict(
            dimension="topic",
            start_date=self.date_delta,
        )
        charts_obj = DemoChart(self.account, filters)
        data = charts_obj.chart_items
        return data['items']

    def get_keyword_data(self):
        filters = dict(
            dimension="keyword",
            start_date=self.date_delta,
        )
        charts_obj = DemoChart(self.account, filters)
        data = charts_obj.chart_items
        return data['items']

    def get_device_data(self):
        filters = dict(
            dimension="device",
            start_date=self.date_delta,
        )
        charts_obj = DemoChart(self.account, filters)
        data = charts_obj.chart_items
        return data['items']
