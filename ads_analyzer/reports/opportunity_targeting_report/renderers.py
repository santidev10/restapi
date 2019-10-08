from typing import Type

from rest_framework_csv.renderers import CSVRenderer

from ads_analyzer.reports.opportunity_targeting_report.styles import Styles
from ads_analyzer.reports.opportunity_targeting_report.styles import TargetSheetTableStyles


class Cursor:
    def __init__(self):
        super().__init__()
        self.row, self.column = 0, 0

    def __str__(self):
        return f"Cursor <{self.row}, {self.column}>"

    def next_row(self, keep_column=False):
        self.row += 1

        if not keep_column:
            self.column = 0

    def next_column(self):
        self.column += 1


class SheetTableRenderer(CSVRenderer):
    sheet_name = None
    style_cls: Type[Styles] = None

    def __init__(self, workbook, sheet_headers=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.workbook = workbook
        self.sheet_headers = sheet_headers or []
        self.cursor = Cursor()
        self.styles = self.style_cls(workbook)

    def render(self, data):
        sheet = self.workbook.add_worksheet(self.sheet_name)
        self._render_header(sheet)
        self._render_empty_line(sheet)
        self._render_table(sheet, data)

    def _render_header(self, sheet):
        cursor = self.cursor
        for row in self.sheet_headers:
            sheet.write(cursor.row, cursor.column, row)
            cursor.next_row()

    def _render_empty_line(self, sheet):
        self.cursor.next_row()

    def _render_table(self, sheet, data):
        table = self.tablize(data, header=self.header, labels=self.labels)
        cursor = self.cursor
        is_header = True
        for row in table:
            for value in row:
                style = self.styles.get_style(cursor, value, is_header, self.header[cursor.column])
                sheet.write(cursor.row, cursor.column, value, style)
                cursor.next_column()
            cursor.next_row()
            is_header = False


class TargetSheetTableRenderer(SheetTableRenderer):
    sheet_name = "Target"
    style_cls = TargetSheetTableStyles
    header = [
        "name",
        "type",
        "campaign_name",
        "ad_group_name",
        "placement_name",
        "placement_start",
        "placement_end",
        "days_remaining",
        "margin_cap",
        "cannot_roll_over",
        "rate_type",
        "contracted_rate",
        "max_bid",
        "avg_rate",
        "cost",
        "cost_delivery_percentage",
        "impressions",
        "video_views",
        "delivery_percentage",
        "revenue",
        "profit",
        "margin",
        "video_played_to_100",
        "view_rate",
        "clicks",
        "ctr",
    ]
    labels = {
        "name": "Target",
        "type": "Type",
        "campaign_name": "Ads Campaign",
        "ad_group_name": "Ads Ad group",
        "placement_name": "Salesforce Placement",
        "placement_start": "Placement Start Date",
        "placement_end": "Placement End Date",
        "days_remaining": "Days remaining",
        "margin_cap": "Margin Cap",
        "cannot_roll_over": "Cannot Roll over Delivery",
        "rate_type": "Rate Type",
        "contracted_rate": "Contracted Rate",
        "max_bid": "Max bid",
        "avg_rate": "Avg. Rate",
        "cost": "Cost",
        "cost_delivery_percentage": "Cost delivery percentage",
        "impressions": "Impressions",
        "video_views": "Views",
        "delivery_percentage": "Delivery percentage",
        "revenue": "Revenue",
        "profit": "Profit",
        "margin": "Margin",
        "video_played_to_100": "Video played to 100%",
        "view_rate": "View rate",
        "clicks": "Clicks",
        "ctr": "CTR",
    }

class DevicesSheetTableRenderer(SheetTableRenderer):
    sheet_name = "Devices"
    style_cls = TargetSheetTableStyles
    header = [
        "type",
        "campaign_name",
        "ad_group_name",
        "placement_name",
        "placement_start",
        "placement_end",
        "days_remaining",
        "margin_cap",
        "cannot_roll_over",
        "rate_type",
        "contracted_rate",
        "max_bid",
        "avg_rate",
        "cost",
        "cost_delivery_percentage",
        "impressions",
        "video_views",
        "delivery_percentage",
        "revenue",
        "profit",
        "margin",
        "video_played_to_100",
        "view_rate",
        "clicks",
        "ctr",
    ]
    labels = {
        "type": "Type",
        "campaign_name": "Ads Campaign",
        "ad_group_name": "Ads Ad group",
        "placement_name": "Salesforce Placement",
        "placement_start": "Placement Start Date",
        "placement_end": "Placement End Date",
        "days_remaining": "Days remaining",
        "margin_cap": "Margin Cap",
        "cannot_roll_over": "Cannot Roll over Delivery",
        "rate_type": "Rate Type",
        "contracted_rate": "Contracted Rate",
        "max_bid": "Max bid",
        "avg_rate": "Avg. Rate",
        "cost": "Cost",
        "cost_delivery_percentage": "Cost delivery percentage",
        "impressions": "Impressions",
        "video_views": "Views",
        "delivery_percentage": "Delivery percentage",
        "revenue": "Revenue",
        "profit": "Profit",
        "margin": "Margin",
        "video_played_to_100": "Video played to 100%",
        "view_rate": "View rate",
        "clicks": "Clicks",
        "ctr": "CTR",
    }
