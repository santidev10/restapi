from io import BytesIO

import xlsxwriter


class PerformanceReportColumn:
    IMPRESSIONS = 2
    VIEWS = 3
    COST = 4
    AVERAGE_CPM = 5
    AVERAGE_CPV = 6
    CLICKS = 7
    CTR_I = 8
    CTR_V = 9
    VIEW_RATE = 10
    QUARTERS = range(11, 15)


class DashboardPerformanceReport:
    columns = (
        ("tab", ""),
        ("name", "Name"),
        ("impressions", "Impressions"),
        ("video_views", "Views"),
        ("cost", "Cost"),
        ("average_cpm", "Average cpm"),
        ("average_cpv", "Average cpv"),
        ("clicks", "Clicks"),
        ("clicks_call_to_action_overlay", "Call-to-Action overlay"),
        ("clicks_website", "Website"),
        ("clicks_app_store", "App Store"),
        ("clicks_cards", "Cards"),
        ("clicks_end_cap", "End cap"),
        ("ctr", "Ctr(i)"),
        ("ctr_v", "Ctr(v)"),
        ("video_view_rate", "View rate"),
        ("video25rate", "25%"),
        ("video50rate", "50%"),
        ("video75rate", "75%"),
        ("video100rate", "100%"),
    )
    columns_width = (10, 40, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10)

    def __init__(self, columns_to_hide=None):
        self._exclude_columns(columns_to_hide or [])

    @property
    def column_names(self):
        return dict(self.columns)

    @property
    def column_keys(self):
        return tuple(key for key, _ in self.columns)

    def _exclude_columns(self, columns_to_hide):
        self.columns = [column for i, column in enumerate(self.columns) if i not in columns_to_hide]
        self.columns_width = [width for i, width in enumerate(self.columns_width) if i not in columns_to_hide]

    def generate(self, data_generator):

        output = BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet()
        for index, width in enumerate(self.columns_width):
            worksheet.set_column(index, index, width)

        self._put_header(worksheet)

        percent_format = workbook.add_format({
            "num_format": "0.00%",
        })
        cell_formats = {
            14: dict(format=percent_format, fn=div_by_100),
            15: dict(format=percent_format, fn=div_by_100),
            16: dict(format=percent_format, fn=div_by_100),
            17: dict(format=percent_format, fn=div_by_100),
            18: dict(format=percent_format, fn=div_by_100),
            19: dict(format=percent_format, fn=div_by_100),
            20: dict(format=percent_format, fn=div_by_100),
        }

        self._write_rows(worksheet, data_generator(), 1, 0, cell_formats)

        workbook.close()

        return output.getvalue()

    def _put_header(self, worksheet):
        self._write_row(worksheet, self.column_names, 0, 0)

    def _write_rows(self, worksheet, data, start_row, start_column=0,
                    cell_formats=None):
        for index, row in enumerate(data):
            self._write_row(worksheet, row, start_row + index, start_column,
                            cell_formats)

    def _write_row(self, worksheet, row, start_row, start_column=0,
                   cell_formats=None):
        cell_formats = cell_formats or {}
        for index, key in enumerate(self.column_keys):
            value = row.get(key)
            current_column = start_column + index
            formatting = cell_formats.get(index, {})
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
