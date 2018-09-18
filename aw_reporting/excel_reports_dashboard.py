import logging
from datetime import datetime
from datetime import timedelta
from functools import partial
from io import BytesIO

import xlsxwriter
from django.conf import settings
from django.db.models import Sum

from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import CLICKS_STATS
from aw_reporting.models import Devices
from aw_reporting.models import GenderStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import Opportunity
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import age_range_str
from aw_reporting.models import all_stats_aggregator
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models import dict_quartiles_to_rates
from aw_reporting.models import gender_str
from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)
all_stats_aggregation = partial(all_stats_aggregator, "ad_group__campaign__")


def get_all_stats_aggregate_with_clicks_stats():
    return {
        **all_stats_aggregation(),
        **{field: Sum(field) for field in CLICKS_STATS}
    }


def div_by_100(value):
    return value / 100. if value is not None else ""


FOOTER_ANNOTATION = "*Other includes YouTube accessed by Smart TV's, Connected TV Devices, Non-smart phones etc."


class PerformanceWeeklyReport:
    _with_cta_columns = (
        "Impressions",
        "Views",
        "View Rate",
        "Clicks",
        "Call-to-Action overlay",
        "Website",
        "App Store",
        "Cards",
        "End cap",
        "CTR",
        "Video played to: 25%",
        "Video played to: 50%",
        "Video played to: 75%",
        "Video played to: 100%",
        # TODO We don't collect the statistic for those two columns yet
        "Viewable Impressions",
        "Viewability",
    )

    _general_columns = (
        "Impressions",
        "Views",
        "View Rate",
        "Clicks",
        "CTR",
        "Video played to: 25%",
        "Video played to: 50%",
        "Video played to: 75%",
        "Video played to: 100%",
        # TODO We don't collect the statistic for those two columns yet
        "Viewable Impressions",
        "Viewability",
    )

    def _extract_data_row_with_cta(self, row):
        return (
            row["impressions"],
            row["video_views"],
            div_by_100(row["video_view_rate"]),
            row["clicks"],
            row["clicks_call_to_action_overlay"],
            row["clicks_website"],
            row["clicks_app_store"],
            row["clicks_cards"],
            row["clicks_end_cap"],
            div_by_100(row["ctr"]),
            div_by_100(row["video25rate"]),
            div_by_100(row["video50rate"]),
            div_by_100(row["video75rate"]),
            div_by_100(row["video100rate"]),
            "",
            ""
        )

    def _extract_data_row_without_cta(self, row):
        return (
            row["impressions"],
            row["video_views"],
            div_by_100(row["video_view_rate"]),
            row["clicks"],
            div_by_100(row["ctr"]),
            div_by_100(row["video25rate"]),
            div_by_100(row["video50rate"]),
            div_by_100(row["video75rate"]),
            div_by_100(row["video100rate"]),
            "",
            ""
        )

    def _set_format_options(self):
        """
        Set default format options
        :return: None
        """
        # Merge cells
        merge_style_options = {
            "border": 1,
            "border_color": "black",
            'valign': 'top'
        }
        self.merge_format = self.workbook.add_format(merge_style_options)

        # Bold text style
        bold_text_options = {
            "bold": True
        }
        self.bold_format = self.workbook.add_format(bold_text_options)

        # Annotation style
        annotation_format_options = {
            "italic": True,
            "locked": False,
            "font_size": 10
        }
        self.annotation_format = self.workbook.add_format(
            annotation_format_options)

        # Header style
        header_format_options = {
            "bold": True,
            "align": "center",
            "bg_color": "#C0C0C0",
            "border": True,
        }
        self.header_format = self.workbook.add_format(header_format_options)

        # Footer style
        footer_text_format = self.workbook.add_format({
            "bold": True,
            "align": "center",
            "bg_color": "#808080",
            "border": True,
        })
        footer_percent_format = self.workbook.add_format({
            "bold": True,
            "align": "center",
            "bg_color": "#808080",
            "border": True,
            "num_format": "0.00%",
        })
        self.footer_format = {
            1: footer_text_format,
            2: footer_text_format,
            3: footer_text_format,
            4: footer_percent_format,
            5: footer_text_format,
            6: footer_text_format,
            7: footer_text_format,
            8: footer_text_format,
            9: footer_text_format,
            10: footer_text_format,
            11: footer_percent_format,
            12: footer_percent_format,
            13: footer_percent_format,
            14: footer_percent_format,
            15: footer_percent_format,
            16: footer_text_format,
            17: footer_text_format,
        }

        # First column cell
        first_column_cell_options = {
            "border": True,
            "align": "left"
        }
        first_column_cell_format = self.workbook.add_format(
            first_column_cell_options)

        # 2-3,5-6, 11 column cell
        middle_columns_cell_options = {
            "border": True,
            "align": "right"
        }
        middle_columns_cell_format = self.workbook.add_format(
            middle_columns_cell_options)

        # 4 column cell
        middle_columns_percentage_cell_options = {
            "border": True,
            "align": "right",
            "num_format": "0.00%",
        }
        middle_columns_percentage_cell_format = self.workbook.add_format(
            middle_columns_percentage_cell_options)

        # 7-10 column cell options
        last_columns_percentage_cell_options = {
            "border": True,
            "align": "center",
            "num_format": "0.00%",
        }
        last_columns_percentage_cell_format = self.workbook.add_format(
            last_columns_percentage_cell_options
        )

        # 12 column cell options
        last_columns_cell_options = {
            "border": True,
            "align": "center",
        }
        last_columns_cell_format = self.workbook.add_format(
            last_columns_cell_options
        )

        self.data_cell_options = {
            1: first_column_cell_format,
            2: middle_columns_cell_format,
            3: middle_columns_cell_format,
            4: middle_columns_percentage_cell_format,
            5: middle_columns_cell_format,
            6: middle_columns_cell_format,
            7: middle_columns_cell_format,
            8: middle_columns_cell_format,
            9: middle_columns_cell_format,
            10: middle_columns_cell_format,
            11: middle_columns_percentage_cell_format,
            12: last_columns_percentage_cell_format,
            13: last_columns_percentage_cell_format,
            14: last_columns_percentage_cell_format,
            15: last_columns_percentage_cell_format,
            # TODO We don't collect the statistic for those two columns yet
            16: middle_columns_cell_format,
            17: last_columns_cell_format,

        }

    def _prepare_empty_document(self):
        """
        Prepare output, workbook, worksheets
        :return: None
        """
        # Prepare document
        self.output = BytesIO()
        self.workbook = xlsxwriter.Workbook(self.output, {'in_memory': True})
        # clean up account name
        bad_characters = '[]:*?\/'
        account_name = self.account.name[:31] if self.account and self.account.name else ""
        for char in account_name:
            if char in bad_characters:
                account_name = account_name.replace(char, "")
        self.worksheet = self.workbook.add_worksheet(
            "{}".format(account_name))
        # Set columns width
        columns_width = {
            0: 3,
            1: 50,
            2: 15,
            3: 10,
            4: 15,
            5: 10,
            6: 20,
            7: 10,
            8: 10,
            9: 10,
            10: 10,
            11: 10,
            12: 25,
            13: 25,
            14: 25,
            15: 25,
            # TODO We don't collect the statistic for those two columns yet
            16: 25,
            17: 25,
        }
        for key, value in columns_width.items():
            self.worksheet.set_column(key, key, value)
        # Set start column & row
        self.start_column = 1
        self.start_row = 12

    def get_filters(self, date_filter=True):
        filters = dict(
            ad_group__campaign__account=self.account,
        )
        if date_filter:
            filters['date__gte'] = self.date_delta

        if self.ad_groups:
            filters['ad_group_id__in'] = self.ad_groups

        elif self.campaigns:
            filters['ad_group__campaign__id__in'] = self.campaigns

        return filters

    def __init__(self, account, campaigns=None, ad_groups=None):
        # Obtain visible campaigns
        self.account = account
        self.campaigns = campaigns or []
        self.ad_groups = ad_groups or []
        self.date_delta = now_in_default_tz().date() - timedelta(days=7)

    def get_content(self):
        # Init document
        self._prepare_empty_document()
        # Prepare global format default options
        self._set_format_options()
        # Filling document
        self.prepare_overview_section()
        next_row = self.prepare_placement_section(self.start_row)
        next_row = self.prepare_video_section(next_row)
        next_row = self.prepare_ages_section(next_row)
        next_row = self.prepare_genders_section(next_row)
        next_row = self.prepare_creatives_section(next_row)
        next_row = self.prepare_ad_group_section(next_row)
        next_row = self.prepare_interest_section(next_row)
        next_row = self.prepare_topic_section(next_row)
        next_row = self.prepare_keyword_section(next_row)
        self.prepare_device_section(next_row)
        # Close document, prepare and return file response
        self.workbook.close()
        return self.output.getvalue()

    def write_rows(self, data, start_row, default_format=None,
                   data_cell_options=None):
        """
        Writing document rows
        :param data: list of lists
        :param start_row: row to start write from
        :param default_format: use default format for all cells
        :return: int
        """
        data_cell_options = data_cell_options or self.data_cell_options
        for row in data:
            for column, value in enumerate(row):
                current_column = self.start_column + column
                if default_format is not None:
                    style = default_format
                else:
                    style = data_cell_options.get(
                        self.start_column + column)
                self.worksheet.write(
                    start_row,
                    current_column,
                    value,
                    style
                )
            start_row += 1
        return start_row

    def prepare_overview_section(self):
        """
        Filling overview section
        :return: None
        """
        logo_path = "{}/{}".format(settings.BASE_DIR, "static/CF_logo.png")
        self.worksheet.insert_image(
            'B2', logo_path, {'x_scale': 0.6, 'y_scale': 0.5})
        opportunity = Opportunity.objects.filter(placements__adwords_campaigns__account=self.account).first()
        # TODO replace N/A
        # campaign
        campaign_title = "Campaign: "
        campaign_data = "{}\n".format(
            self.account.name) if self.account else "N/A"
        # flight
        flight_title = "Flight: "
        flight_start_date = self.account.start_date.strftime("%m/%d/%y") \
            if self.account and self.account.start_date is not None else "N/A"
        flight_end_date = self.account.end_date.strftime("%m/%d/%y") \
            if self.account and self.account.end_date is not None else "N/A"
        flight_data = "{} - {}\n".format(flight_start_date, flight_end_date)
        # budget
        budget_title = "Client Budget: "
        budget_data = "N/A\n"
        if opportunity is not None and opportunity.budget is not None:
            budget_data = "${}\n".format(opportunity.budget)
        # rates
        rates_title = "Contracted Rates: "
        cpv_data = "N/A"
        cpm_data = "N/A"
        if opportunity is not None:
            if opportunity.contracted_cpv:
                cpv_data = "${}".format(opportunity.contracted_cpv)
            if opportunity.contracted_cpm:
                cpm_data = "${}".format(opportunity.contracted_cpm)
        rates_data = "CPV {} / CPM {}\n".format(cpv_data, cpm_data)
        # contracted views
        contracted_units_title = "Contracted Units: "
        contracted_views_data = "N/A"
        contracted_impressions_data = "N/A"
        if opportunity is not None:
            if opportunity.video_views:
                contracted_views_data = "ordered CPV units = {} views".format(opportunity.video_views)
            if opportunity.impressions:
                contracted_impressions_data = "ordered CPM units = {} impressions".format(opportunity.impressions)
        contracted_units_data = "{} / {}\n".format(contracted_views_data, contracted_impressions_data)
        # reporting date range
        reporting_date_range_title = "Reporting date range: "
        reporting_date_range_data = "{} - {}".format(
            self.date_delta.strftime("%m/%d/%y"),
            (datetime.now().date() - timedelta(days=1)).strftime("%m/%d/%y"))
        # Set merge area
        # pylint: disable=no-value-for-parameter
        self.worksheet.merge_range('B1:D4', "")
        self.worksheet.merge_range('B5:D11', "", self.merge_format)
        # pylint: enable=no-value-for-parameter
        self.worksheet.write_rich_string(
            "B5",
            self.bold_format,
            campaign_title,
            campaign_data,
            self.bold_format,
            flight_title,
            flight_data,
            self.bold_format,
            budget_title,
            budget_data,
            self.bold_format,
            rates_title,
            rates_data,
            self.bold_format,
            contracted_units_title,
            contracted_units_data,
            self.bold_format,
            reporting_date_range_title,
            reporting_date_range_data,
            self.merge_format
        )
        # TODO add brand image

    def get_campaign_data(self):
        queryset = AdGroupStatistic.objects.filter(**self.get_filters())
        group_by = ("ad_group__campaign__name", "ad_group__campaign_id")
        campaign_data = queryset.values(*group_by).annotate(
            **get_all_stats_aggregate_with_clicks_stats()
        ).order_by(*group_by)
        for i in campaign_data:
            i['name'] = i['ad_group__campaign__name']
            dict_norm_base_stats(i)
            dict_add_calculated_stats(i)
            dict_quartiles_to_rates(i)
        return campaign_data

    def get_total_data(self):
        queryset = AdGroupStatistic.objects.filter(**self.get_filters())
        total_data = queryset.aggregate(
            **get_all_stats_aggregate_with_clicks_stats()
        )
        dict_norm_base_stats(total_data)
        dict_add_calculated_stats(total_data)
        dict_quartiles_to_rates(total_data)
        return total_data

    def prepare_placement_section(self, start_row):
        """
        Filling placement section
        :param start_row: row to start write from
        :return: int
        """
        # Write header
        headers = [(
            "Placement",
            *self._with_cta_columns,
        )]
        start_row = self.write_rows(headers, start_row, self.header_format)
        # Write content

        rows = []
        for obj in self.get_campaign_data():
            rows.append((
                # placement
                obj["name"],
                *self._extract_data_row_with_cta(obj),
            ))
        start_row = self.write_rows(rows, start_row)
        # Write total
        total_data = self.get_total_data()
        # Drop None values
        total_row = [(
            "Total",
            total_data["impressions"],
            *self._extract_data_row_with_cta(total_data),
        )]
        start_row = self.write_rows(
            total_row, start_row, data_cell_options=self.footer_format)
        return start_row + 1

    def get_ad_group_data(self):
        queryset = AdGroupStatistic.objects.filter(**self.get_filters())
        group_by = ("ad_group__name", "ad_group_id")
        campaign_data = queryset.values(*group_by).annotate(
            **get_all_stats_aggregate_with_clicks_stats()
        ).order_by(*group_by)
        for i in campaign_data:
            i['name'] = i['ad_group__name']
            dict_norm_base_stats(i)
            dict_add_calculated_stats(i)
            dict_quartiles_to_rates(i)
        return campaign_data

    def prepare_ad_group_section(self, start_row):
        """
        Filling ad group section
        :param start_row: row to start write from
        :return: int
        """
        # Write header
        headers = [(
            "Ad Groups",
            *self._with_cta_columns,
        )]
        start_row = self.write_rows(headers, start_row, self.header_format)
        # Write content
        ad_group_info = [
            (
                obj["name"],
                *self._extract_data_row_with_cta(obj),
            )
            for obj in self.get_ad_group_data()
        ]
        start_row = self.write_rows(ad_group_info, start_row)
        return start_row + 1

    def get_video_data(self):
        queryset = YTVideoStatistic.objects.filter(**self.get_filters())
        videos_data = queryset \
            .values("yt_id") \
            .annotate(**all_stats_aggregation()) \
            .order_by("yt_id")
        videos_data = list(videos_data)
        ids = [i["yt_id"] for i in videos_data]
        videos_info = {}
        connector = SingleDatabaseApiConnector()
        try:
            items = connector.get_videos_base_info(ids)
        except SingleDatabaseApiConnectorException as e:
            logger.error(e)
        else:
            videos_info = {i['id']: i for i in items}
        for item in videos_data:
            video_id = item["yt_id"]
            item['name'] = videos_info.get(video_id, {}).get("title", video_id)
            dict_norm_base_stats(item)
            dict_add_calculated_stats(item)
            dict_quartiles_to_rates(item)
        return videos_data

    def prepare_video_section(self, start_row):
        """
        Filling interest section
        :param start_row: row to start write from
        :return: int
        """
        # Write header
        headers = [(
            "Video",
            *self._general_columns,
        )]
        start_row = self.write_rows(headers, start_row, self.header_format)
        # Write content
        rows = [
            (
                obj["name"],
                *self._extract_data_row_without_cta(obj),
            )
            for obj in self.get_video_data()
        ]
        start_row = self.write_rows(rows, start_row)
        return start_row + 1

    def get_ages_data(self):
        queryset = AgeRangeStatistic.objects.filter(**self.get_filters())
        ages_data = queryset \
            .values("age_range_id") \
            .annotate(**get_all_stats_aggregate_with_clicks_stats()) \
            .order_by("age_range_id")
        for item in ages_data:
            item["name"] = age_range_str(item["age_range_id"])
            dict_norm_base_stats(item)
            dict_add_calculated_stats(item)
            dict_quartiles_to_rates(item)
        return ages_data

    def prepare_ages_section(self, start_row):
        headers = [(
            "Ages",
            *self._with_cta_columns,
        )]
        start_row = self.write_rows(headers, start_row, self.header_format)
        rows = [
            (
                obj["name"],
                *self._extract_data_row_with_cta(obj),
            )
            for obj in self.get_ages_data()
        ]
        start_row = self.write_rows(rows, start_row)
        return start_row + 1

    def get_genders_data(self):
        queryset = GenderStatistic.objects.filter(**self.get_filters())
        ages_data = queryset \
            .values("gender_id") \
            .annotate(**get_all_stats_aggregate_with_clicks_stats()) \
            .order_by("gender_id")
        for item in ages_data:
            item["name"] = gender_str(item["gender_id"])
            dict_norm_base_stats(item)
            dict_add_calculated_stats(item)
            dict_quartiles_to_rates(item)
        return ages_data

    def prepare_genders_section(self, start_row):
        headers = [(
            "Genders",
            *self._with_cta_columns,
        )]
        start_row = self.write_rows(headers, start_row, self.header_format)
        rows = [
            (
                obj["name"],
                *self._extract_data_row_with_cta(obj),
            )
            for obj in self.get_genders_data()
        ]
        start_row = self.write_rows(rows, start_row)
        return start_row + 1

    def get_creatives_data(self):
        queryset = VideoCreativeStatistic.objects.filter(**self.get_filters())
        videos_data = queryset \
            .values("creative_id") \
            .annotate(**all_stats_aggregation()) \
            .order_by("creative_id")
        videos_data = list(videos_data)
        ids = [i["creative_id"] for i in videos_data]
        videos_info = {}
        connector = SingleDatabaseApiConnector()
        try:
            items = connector.get_videos_base_info(ids)
        except SingleDatabaseApiConnectorException as e:
            logger.error(e)
        else:
            videos_info = {i['id']: i for i in items}
        for item in videos_data:
            video_id = item["creative_id"]
            item['name'] = videos_info.get(video_id, {}).get("title", video_id)
            dict_norm_base_stats(item)
            dict_add_calculated_stats(item)
            dict_quartiles_to_rates(item)
        return videos_data

    def prepare_creatives_section(self, start_row):
        headers = [(
            "Creatives",
            *self._general_columns,
        )]
        start_row = self.write_rows(headers, start_row, self.header_format)
        rows = [
            (
                obj["name"],
                *self._extract_data_row_without_cta(obj),
            )
            for obj in self.get_creatives_data()
        ]
        start_row = self.write_rows(rows, start_row)
        return start_row + 1

    def get_interest_data(self):
        queryset = AudienceStatistic.objects.filter(**self.get_filters())
        interest_data = queryset.values("audience__name").annotate(
            **get_all_stats_aggregate_with_clicks_stats()
        ).order_by("audience__name")
        for i in interest_data:
            i['name'] = i['audience__name']
            dict_norm_base_stats(i)
            dict_add_calculated_stats(i)
            dict_quartiles_to_rates(i)
        return interest_data

    def prepare_interest_section(self, start_row):
        """
        Filling interest section
        :param start_row: row to start write from
        :return: int
        """
        # Write header
        headers = [(
            "Interests",
            *self._with_cta_columns,
        )]
        start_row = self.write_rows(headers, start_row, self.header_format)
        # Write content
        rows = [
            (
                obj["name"],
                *self._extract_data_row_with_cta(obj),
            )
            for obj in self.get_interest_data()
        ]
        start_row = self.write_rows(rows, start_row)
        return start_row + 1

    def get_topic_data(self):
        queryset = TopicStatistic.objects.filter(**self.get_filters())
        topic_data = queryset.values("topic__name").order_by(
            "topic__name").annotate(
            **get_all_stats_aggregate_with_clicks_stats()
        )
        for i in topic_data:
            i['name'] = i['topic__name']
            dict_norm_base_stats(i)
            dict_add_calculated_stats(i)
            dict_quartiles_to_rates(i)
        return topic_data

    def prepare_topic_section(self, start_row):
        """
        Filling topic section
        :param start_row: row to start write from
        :return: int
        """
        # Write header
        headers = [(
            "Topics",
            *self._with_cta_columns,
        )]
        start_row = self.write_rows(headers, start_row, self.header_format)
        # Write content

        rows = [
            (
                obj["name"],
                *self._extract_data_row_with_cta(obj),
            )
            for obj in self.get_topic_data()
        ]
        start_row = self.write_rows(rows, start_row)
        return start_row + 1

    def get_keyword_data(self):
        queryset = KeywordStatistic.objects.filter(**self.get_filters())
        keyword_data = queryset.values("keyword").annotate(
            **get_all_stats_aggregate_with_clicks_stats()
        ).order_by("keyword")
        for i in keyword_data:
            i['name'] = i['keyword']
            dict_norm_base_stats(i)
            dict_add_calculated_stats(i)
            dict_quartiles_to_rates(i)
        return keyword_data

    def prepare_keyword_section(self, start_row):
        """
        Filling keyword section
        :param start_row: row to start write from
        :return: int
        """
        # Write header
        headers = [(
            "Keywords",
            *self._with_cta_columns,
        )]
        start_row = self.write_rows(headers, start_row, self.header_format)
        # Write content
        rows = [
            (
                obj['name'],
                *self._extract_data_row_with_cta(obj),
            )
            for obj in self.get_keyword_data()
        ]
        start_row = self.write_rows(rows, start_row)
        return start_row + 1

    def get_device_data(self):
        queryset = AdGroupStatistic.objects.filter(**self.get_filters())
        device_data = queryset.values("device_id").annotate(
            **get_all_stats_aggregate_with_clicks_stats()
        ).order_by("device_id")
        for i in device_data:
            i['name'] = Devices[i['device_id']]
            dict_norm_base_stats(i)
            dict_add_calculated_stats(i)
            dict_quartiles_to_rates(i)
        return device_data

    def prepare_device_section(self, start_row):
        """
        Filling device section
        :param start_row: row to start write from
        :return: None
        """
        # Write header
        headers = [(
            "Device",
            *self._with_cta_columns,
        )]
        start_row = self.write_rows(headers, start_row, self.header_format)
        # Write content

        rows = []
        for obj in self.get_device_data():
            device = obj['name']
            if device == "Other":
                device = "Other*"
            rows.append(
                (
                    device,
                    *self._extract_data_row_with_cta(obj),
                )
            )
        start_row = self.write_rows(rows, start_row)
        # Write annotation

        annotation_row = [
            [FOOTER_ANNOTATION]
        ]
        self.write_rows(annotation_row, start_row, self.annotation_format)


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


class PerformanceReport:
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
