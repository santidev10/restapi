import re
from copy import copy
from datetime import datetime
from functools import partial

from django.db.models import Sum
from django.http import HttpResponseBadRequest
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.calculations.cost import get_client_cost_aggregation
from aw_reporting.dashboard_charts import DateSegment
from aw_reporting.dashboard_charts import DeliveryChart
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.excel_reports import DashboardPerformanceReport
from aw_reporting.excel_reports.dashboard_performance_report import DashboardPerformanceReportColumn
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import CLICKS_STATS
from aw_reporting.models import Campaign
from aw_reporting.models import DATE_FORMAT
from aw_reporting.models import all_stats_aggregator
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models import dict_quartiles_to_rates
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.lang import ExtendedEnum
from utils.permissions import UserHasDashboardPermission
from utils.views import xlsx_response


@demo_view_decorator
class DashboardPerformanceExportApiView(APIView):
    permission_classes = (IsAuthenticated, UserHasDashboardPermission)

    def post(self, request, pk, **_):
        try:
            self._validate_request_payload()
        except ValueError as ex:
            return HttpResponseBadRequest(ex)
        filters = {}
        user_settings = request.user.get_aw_settings()
        visible_all_accounts = user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS)
        if not visible_all_accounts:
            visible_accounts = user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS) or []
            filters["account__id__in"] = visible_accounts
        try:
            item = AccountCreation.objects.filter(**filters).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        data_generator = partial(self.get_export_data, item, request.user)
        account_name = item.account.name if item.account is not None else item.name
        return self.build_response(account_name, data_generator)

    tabs = (
        "device", "gender", "age", "topic", "interest", "remarketing",
        "keyword", "location", "creative", "ad", "channel", "video",
    )

    def build_response(self, account_name, data_generator):
        title = "Segmented report {account_name} {timestamp}".format(
            account_name=re.sub(r"\W", account_name, "-"),
            timestamp=now_in_default_tz().strftime("%Y%m%d"),
        )
        user = self.request.user

        xls_report = DashboardPerformanceReport(
            custom_header=self._get_custom_header(),
            columns_to_hide=self._get_columns_to_hide(user),
            date_format_str=self._get_date_segment_format()
        )
        return xlsx_response(title, xls_report.generate(data_generator))

    def _get_custom_header(self):
        filters = self.get_filters()
        campaign_names = Campaign.objects \
            .filter(id__in=filters.get("campaigns") or []) \
            .values_list("name", flat=True)
        ad_group_names = AdGroup.objects \
            .filter(id__in=filters.get("ad_groups") or []) \
            .values_list("name", flat=True)
        header_rows = [
            "Date: {start_date} - {end_date}",
            "Group By: {metric}",
            "Campaigns: {campaigns}",
            "Ad Groups: {ad_groups}",
        ]
        header_date = dict(
            metric=METRIC_REPRESENTATION.get(self._get_metric()),
            start_date=filters.get("start_date"),
            end_date=filters.get("end_date"),
            campaigns=", ".join(campaign_names),
            ad_groups=", ".join(ad_group_names),
        )
        return "\n".join(header_rows).format(**header_date)

    def _get_columns_to_hide(self, user):
        columns_to_hide = []
        hide_costs = user.get_aw_settings().get(UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN)
        if hide_costs:
            columns_to_hide = columns_to_hide + [DashboardPerformanceReportColumn.COST,
                                                 DashboardPerformanceReportColumn.AVERAGE_CPM,
                                                 DashboardPerformanceReportColumn.AVERAGE_CPV]
        date_segment = self._get_date_segment()
        if not date_segment:
            columns_to_hide = columns_to_hide + [DashboardPerformanceReportColumn.DATE_SEGMENT]
        show_conversions = user.get_aw_settings().get(UserSettingsKey.SHOW_CONVERSIONS)
        if not show_conversions:
            columns_to_hide = columns_to_hide + [DashboardPerformanceReportColumn.ALL_CONVERSIONS]
        return columns_to_hide

    def _get_date_segment_format(self):
        date_segment = self._get_date_segment()
        return DATE_SEGMENT_STRFTIME_FORMAT.get(date_segment)

    def get_filters(self):
        data = self.request.data
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        filters = dict(
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
            campaigns=data.get("campaigns"),
            ad_groups=data.get("ad_groups"),
        )
        return filters

    def get_export_data(self, item, user):
        filters = self.get_filters()
        data = dict(name=item.name)

        account = item.account

        fs = {"ad_group__campaign__account": account}
        if filters["start_date"]:
            fs["date__gte"] = filters["start_date"]
        if filters["end_date"]:
            fs["date__lte"] = filters["end_date"]
        if filters["ad_groups"]:
            fs["ad_group_id__in"] = filters["ad_groups"]
        elif filters["campaigns"]:
            fs["ad_group__campaign_id__in"] = filters["campaigns"]

        aggregation = copy(all_stats_aggregator("ad_group__campaign__"))
        for field in CLICKS_STATS:
            aggregation["sum_{}".format(field)] = Sum(field)

        user_settings = user.get_aw_settings()
        show_aw_rates = user_settings.get(UserSettingsKey.DASHBOARD_AD_WORDS_RATES)
        if not show_aw_rates:
            aggregation["sum_cost"] = get_client_cost_aggregation()
        stats = AdGroupStatistic.objects.filter(**fs).aggregate(**aggregation)

        dict_norm_base_stats(stats)
        dict_quartiles_to_rates(stats)
        dict_add_calculated_stats(stats)
        data.update(stats)

        yield {**{"tab": "Summary"}, **data}

        accounts = []
        if account:
            accounts.append(account.id)

        for dimension in self._get_tabs():
            chart = DeliveryChart(
                accounts=accounts,
                dimension=dimension,
                show_aw_costs=show_aw_rates,
                date_segment=self._get_date_segment(),
                **filters
            )
            items = chart.get_items()
            for data in items["items"]:
                metric = DIMENSION_MAP[dimension]
                tab_name = METRIC_REPRESENTATION[metric]
                yield {**{"tab": tab_name}, **data}

    def _get_tabs(self):
        metric = self._get_metric()
        if metric is None:
            return self.tabs
        return [METRIC_MAP[metric]]

    def _validate_request_payload(self):
        metric = self._get_metric_parameter()
        if metric not in ALLOWED_METRICS:
            raise ValueError("Wrong metric")
        date_segment = self._get_date_segment()
        if date_segment not in ALLOWED_DATE_SEGMENT:
            raise ValueError("Wrong date_segment")

    def _get_metric(self):
        metric_parameter = self._get_metric_parameter()
        return Metric(metric_parameter) \
            if metric_parameter \
            else None

    def _get_metric_parameter(self):
        return self.request.data.get("metric")

    def _get_date_segment(self):
        return self.request.data.get("date_segment")


class Metric(ExtendedEnum):
    GENDER = "gender"
    AGE = "age"
    LOCATION = "location"
    DEVICE = "device"
    TOPIC = "topic"
    INTEREST = "interest"
    KEYWORD = "keyword"
    CHANNEL = "channel"
    VIDEO = "video"
    CREATIVE = "creative"
    AD_GROUP = "ad_group"
    AUDIENCE = "audience"


METRIC_MAP = {
    Metric.GENDER: "gender",
    Metric.AGE: "age",
    Metric.LOCATION: "location",
    Metric.DEVICE: "device",
    Metric.TOPIC: "topic",
    Metric.INTEREST: "interest",
    Metric.KEYWORD: "keyword",
    Metric.CHANNEL: "channel",
    Metric.VIDEO: "video",
    Metric.CREATIVE: "creative",
    Metric.AD_GROUP: "ad",
    Metric.AUDIENCE: "remarketing"
}
DIMENSION_MAP = {value: key for key, value in METRIC_MAP.items()}
METRIC_REPRESENTATION = {
    Metric.GENDER: "Gender",
    Metric.AGE: "Age",
    Metric.LOCATION: "Location",
    Metric.DEVICE: "Device",
    Metric.TOPIC: "Topic",
    Metric.INTEREST: "Interest",
    Metric.KEYWORD: "Keyword",
    Metric.CHANNEL: "Channel",
    Metric.VIDEO: "Video",
    Metric.CREATIVE: "Creative",
    Metric.AD_GROUP: "Ad Group",
    Metric.AUDIENCE: "Audience"
}
ALLOWED_METRICS = tuple(Metric.values()) + (None,)
ALLOWED_DATE_SEGMENT = tuple(DateSegment.values()) + (None,)
DATE_SEGMENT_STRFTIME_FORMAT = {
    DateSegment.DAY.value: "%m/%d/%Y",
    DateSegment.WEEK.value: "%w",
    DateSegment.MONTH.value: "%b-%y",
    DateSegment.QUARTER.value: "%Y Q%Q",
    DateSegment.YEAR.value: "%Y",
}
