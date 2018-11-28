from io import BytesIO

import xlsxwriter

from utils.lang import ExtendedEnum


class AnalyticsPerformanceReportColumn(ExtendedEnum):
    TAB = "tab"
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


COLUMN_NAME = {
    AnalyticsPerformanceReportColumn.TAB: "",
    AnalyticsPerformanceReportColumn.NAME: "Name",
    AnalyticsPerformanceReportColumn.IMPRESSIONS: "Impressions",
    AnalyticsPerformanceReportColumn.VIEWS: "Views",
    AnalyticsPerformanceReportColumn.COST: "Cost",
    AnalyticsPerformanceReportColumn.AVERAGE_CPM: "Average cpm",
    AnalyticsPerformanceReportColumn.AVERAGE_CPV: "Average cpv",
    AnalyticsPerformanceReportColumn.CLICKS: "Clicks",
    AnalyticsPerformanceReportColumn.CLICKS_CTA_OVERLAY: "Call-to-Action overlay",
    AnalyticsPerformanceReportColumn.CLICKS_CTA_WEBSITE: "Website",
    AnalyticsPerformanceReportColumn.CLICKS_CTA_APP_STORE: "App Store",
    AnalyticsPerformanceReportColumn.CLICKS_CTA_CARDS: "Cards",
    AnalyticsPerformanceReportColumn.CLICKS_CTA_END_CAP: "End cap",
    AnalyticsPerformanceReportColumn.CTR_I: "Ctr(i)",
    AnalyticsPerformanceReportColumn.CTR_V: "Ctr(v)",
    AnalyticsPerformanceReportColumn.VIEW_RATE: "View rate",
    AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_25: "25%",
    AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_50: "50%",
    AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_75: "75%",
    AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_100: "100%",
}

COLUMN_WIDTH = {
    AnalyticsPerformanceReportColumn.NAME: 40,
}
DEFAULT_WIDTH = 10
FILTER_ROW_HEIGHT = 50

ALL_COLUMNS = (
    AnalyticsPerformanceReportColumn.TAB,
    AnalyticsPerformanceReportColumn.NAME,
    AnalyticsPerformanceReportColumn.IMPRESSIONS,
    AnalyticsPerformanceReportColumn.VIEWS,
    AnalyticsPerformanceReportColumn.COST,
    AnalyticsPerformanceReportColumn.AVERAGE_CPM,
    AnalyticsPerformanceReportColumn.AVERAGE_CPV,
    AnalyticsPerformanceReportColumn.CLICKS,
    AnalyticsPerformanceReportColumn.CLICKS_CTA_OVERLAY,
    AnalyticsPerformanceReportColumn.CLICKS_CTA_WEBSITE,
    AnalyticsPerformanceReportColumn.CLICKS_CTA_APP_STORE,
    AnalyticsPerformanceReportColumn.CLICKS_CTA_CARDS,
    AnalyticsPerformanceReportColumn.CLICKS_CTA_END_CAP,
    AnalyticsPerformanceReportColumn.CTR_I,
    AnalyticsPerformanceReportColumn.CTR_V,
    AnalyticsPerformanceReportColumn.VIEW_RATE,
    AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_25,
    AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_50,
    AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_75,
    AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_100,
)


class AnalyticsPerformanceReport:

    def __init__(self, columns_to_hide=None):
        self._exclude_columns(columns_to_hide or [])

    def _exclude_columns(self, columns_to_hide):
        self.columns = [column for column in ALL_COLUMNS if column not in columns_to_hide]

    def generate(self, data_generator):

        output = BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet()
        for index, column in enumerate(self.columns):
            width = COLUMN_WIDTH.get(column, DEFAULT_WIDTH)
            worksheet.set_column(index, index, width)

        current_row = self._put_table_header(worksheet, 0)

        percent_format = workbook.add_format({
            "num_format": "0.00%",
        })
        cell_formats = {
            AnalyticsPerformanceReportColumn.VIEW_RATE: dict(format=percent_format, fn=div_by_100),
            AnalyticsPerformanceReportColumn.CTR_I: dict(format=percent_format, fn=div_by_100),
            AnalyticsPerformanceReportColumn.CTR_V: dict(format=percent_format, fn=div_by_100),
            AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_25: dict(format=percent_format, fn=div_by_100),
            AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_50: dict(format=percent_format, fn=div_by_100),
            AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_75: dict(format=percent_format, fn=div_by_100),
            AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_100: dict(format=percent_format, fn=div_by_100),
        }

        self._write_rows(worksheet, data_generator(), current_row, 0, cell_formats)

        workbook.close()

        return output.getvalue()

    def _put_table_header(self, worksheet, start_from):
        header_row = {column.value: value for column, value in COLUMN_NAME.items()}
        self._write_row(worksheet, header_row, start_from, 0)
        return start_from + 1

    def _write_rows(self, worksheet, data, start_row, start_column=0,
                    cell_formats=None):
        for index, row in enumerate(data):
            self._write_row(worksheet, row, start_row + index, start_column,
                            cell_formats)

    def _write_row(self, worksheet, row, start_row, start_column=0,
                   cell_formats=None):
        cell_formats = cell_formats or {}
        for index, column in enumerate(self.columns):
            if index > 10:
                a = 1 + 1
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


def div_by_100(value):
    return value / 100. if value is not None else ""
