from datetime import date
from datetime import datetime
from functools import partial
from io import BytesIO

import xlsxwriter
from django.conf import settings

from utils.datetime import strftime_extended
from utils.lang import ExtendedEnum

TOO_MUCH_DATA_MESSAGE = "The list is too long to be shown entirely. " \
                        "Try to change the time range or particular metrics."


class DashboardPerformanceReportColumn(ExtendedEnum):
    TAB = "tab"
    DATE_SEGMENT = "date_segment"
    NAME = "name"
    IMPRESSIONS = "impressions"
    VIEWS = "video_views"
    COST = "cost"
    AVERAGE_CPM = "average_cpm"
    AVERAGE_CPV = "average_cpv"
    CLICKS = "clicks"
    CTR_I = "ctr"
    CTR_V = "ctr_v"
    VIEW_RATE = "video_view_rate"
    CLICKS_CTA_OVERLAY = "clicks_call_to_action_overlay"
    CLICKS_CTA_WEBSITE = "clicks_website"
    CLICKS_CTA_APP_STORE = "clicks_app_store"
    CLICKS_CTA_CARDS = "clicks_cards"
    CLICKS_CTA_END_CAP = "clicks_end_cap"
    VIDEO_QUARTILE_25 = "video25rate"
    VIDEO_QUARTILE_50 = "video50rate"
    VIDEO_QUARTILE_75 = "video75rate"
    VIDEO_QUARTILE_100 = "video100rate"
    ALL_CONVERSIONS = "all_conversions"


COLUMN_NAME = {
    DashboardPerformanceReportColumn.TAB: "",
    DashboardPerformanceReportColumn.DATE_SEGMENT: "Date",
    DashboardPerformanceReportColumn.NAME: "Name",
    DashboardPerformanceReportColumn.IMPRESSIONS: "Impressions",
    DashboardPerformanceReportColumn.VIEWS: "Views",
    DashboardPerformanceReportColumn.COST: "Cost",
    DashboardPerformanceReportColumn.AVERAGE_CPM: "Average cpm",
    DashboardPerformanceReportColumn.AVERAGE_CPV: "Average cpv",
    DashboardPerformanceReportColumn.CLICKS: "Clicks",
    DashboardPerformanceReportColumn.CLICKS_CTA_OVERLAY: "Call-to-Action overlay",
    DashboardPerformanceReportColumn.CLICKS_CTA_WEBSITE: "Website",
    DashboardPerformanceReportColumn.CLICKS_CTA_APP_STORE: "App Store",
    DashboardPerformanceReportColumn.CLICKS_CTA_CARDS: "Cards",
    DashboardPerformanceReportColumn.CLICKS_CTA_END_CAP: "End cap",
    DashboardPerformanceReportColumn.CTR_I: "Ctr(i)",
    DashboardPerformanceReportColumn.CTR_V: "Ctr(v)",
    DashboardPerformanceReportColumn.VIEW_RATE: "View rate",
    DashboardPerformanceReportColumn.VIDEO_QUARTILE_25: "25%",
    DashboardPerformanceReportColumn.VIDEO_QUARTILE_50: "50%",
    DashboardPerformanceReportColumn.VIDEO_QUARTILE_75: "75%",
    DashboardPerformanceReportColumn.VIDEO_QUARTILE_100: "100%",
    DashboardPerformanceReportColumn.ALL_CONVERSIONS: "All conversions",
}

COLUMN_WIDTH = {
    DashboardPerformanceReportColumn.NAME: 40,
}
DEFAULT_WIDTH = 10
FILTER_ROW_HEIGHT = 50

ALL_COLUMNS = (
    DashboardPerformanceReportColumn.TAB,
    DashboardPerformanceReportColumn.DATE_SEGMENT,
    DashboardPerformanceReportColumn.NAME,
    DashboardPerformanceReportColumn.IMPRESSIONS,
    DashboardPerformanceReportColumn.VIEWS,
    DashboardPerformanceReportColumn.COST,
    DashboardPerformanceReportColumn.AVERAGE_CPM,
    DashboardPerformanceReportColumn.AVERAGE_CPV,
    DashboardPerformanceReportColumn.CLICKS,
    DashboardPerformanceReportColumn.CLICKS_CTA_OVERLAY,
    DashboardPerformanceReportColumn.CLICKS_CTA_WEBSITE,
    DashboardPerformanceReportColumn.CLICKS_CTA_APP_STORE,
    DashboardPerformanceReportColumn.CLICKS_CTA_CARDS,
    DashboardPerformanceReportColumn.CLICKS_CTA_END_CAP,
    DashboardPerformanceReportColumn.CTR_I,
    DashboardPerformanceReportColumn.CTR_V,
    DashboardPerformanceReportColumn.VIEW_RATE,
    DashboardPerformanceReportColumn.VIDEO_QUARTILE_25,
    DashboardPerformanceReportColumn.VIDEO_QUARTILE_50,
    DashboardPerformanceReportColumn.VIDEO_QUARTILE_75,
    DashboardPerformanceReportColumn.VIDEO_QUARTILE_100,
    DashboardPerformanceReportColumn.ALL_CONVERSIONS,
)


class DashboardPerformanceReport:

    def __init__(self, custom_header, columns_to_hide=None, date_format_str=""):
        self.custom_header = custom_header
        self.columns = []
        self._exclude_columns(columns_to_hide or [])
        self.date_format_str = date_format_str

    def _exclude_columns(self, columns_to_hide):
        self.columns = [column for column in ALL_COLUMNS if column not in columns_to_hide]

    def generate(self, data_generator):

        output = BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet()
        for index, column in enumerate(self.columns):
            width = COLUMN_WIDTH.get(column, DEFAULT_WIDTH)
            worksheet.set_column(index, index, width)

        current_row = self._put_custom_headers(worksheet, 0)
        current_row = self._put_table_header(worksheet, current_row)

        percent_format = workbook.add_format({
            "num_format": "0.00%",
        })
        date_segment_format_fn = partial(safe_date_format, strftime_format=self.date_format_str)
        cell_formats = {
            DashboardPerformanceReportColumn.DATE_SEGMENT: dict(fn=date_segment_format_fn),
            DashboardPerformanceReportColumn.VIEW_RATE: dict(format=percent_format, fn=div_by_100),
            DashboardPerformanceReportColumn.VIDEO_QUARTILE_25: dict(format=percent_format, fn=div_by_100),
            DashboardPerformanceReportColumn.VIDEO_QUARTILE_50: dict(format=percent_format, fn=div_by_100),
            DashboardPerformanceReportColumn.VIDEO_QUARTILE_75: dict(format=percent_format, fn=div_by_100),
            DashboardPerformanceReportColumn.VIDEO_QUARTILE_100: dict(format=percent_format, fn=div_by_100),
        }

        self._write_rows(worksheet, data_generator(), current_row, 0, cell_formats)

        workbook.close()

        return output.getvalue()

    def _put_custom_headers(self, worksheet, start_from):
        worksheet.set_row(start_from, FILTER_ROW_HEIGHT)
        worksheet.write(start_from, 1, self.custom_header)
        return start_from + 1

    def _put_table_header(self, worksheet, start_from):
        header_row = {column.value: value for column, value in COLUMN_NAME.items()}
        self._write_row(worksheet, header_row, start_from, 0)
        return start_from + 1

    def _write_rows(self, worksheet, data, start_row, start_column=0,
                    cell_formats=None):

        rows_limit = settings.DASHBOARD_PERFORMANCE_REPORT_LIMIT
        current_row_index = start_row
        for row, index in zip(data, range(start_row, rows_limit)):
            current_row_index = index
            self._write_row(worksheet, row, current_row_index, start_column, cell_formats)

        if current_row_index == rows_limit - 1:
            current_row_index += 1
            worksheet.write(current_row_index, 0, TOO_MUCH_DATA_MESSAGE)

        return current_row_index

    def _write_row(self, worksheet, row, start_row, start_column=0,
                   cell_formats=None):
        cell_formats = cell_formats or {}
        for index, column in enumerate(self.columns):
            value = row.get(column.value)
            current_column = start_column + index
            formatting = cell_formats.get(column, {})
            style = formatting.get("format")
            fn = formatting.get("fn", lambda x: x)
            worksheet.write(
                start_row,
                current_column,
                fn(value),
                style
            )


def safe_date_format(value, strftime_format):
    if value is None:
        return value
    if not isinstance(value, (date, datetime)):
        return str(value)
    return strftime_extended(value, strftime_format)


def div_by_100(value):
    return value / 100. if value is not None else ""
