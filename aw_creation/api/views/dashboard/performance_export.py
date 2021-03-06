import re
from copy import copy
from datetime import datetime
from functools import partial

from django.db.models import Max
from django.db.models import Min
from django.db.models import Sum
from django.http import Http404
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.calculations.cost import get_client_cost_aggregation
from aw_reporting.charts.dashboard_charts import DateSegment
from aw_reporting.charts.dashboard_charts import DeliveryChart
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
from userprofile.constants import StaticPermissions
from utils.api.exceptions import BadRequestError
from utils.api.exceptions import PermissionsError
from utils.datetime import now_in_default_tz
from utils.lang import ExtendedEnum
from utils.views import xlsx_response


class DashboardPerformanceExportApiView(APIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MANAGED_SERVICE__EXPORT),)

    def post(self, request, pk, **_):
        self._validate_request_payload()
        item = self._get_account_creation(request, pk)
        account = item.account
        data_generator = partial(self.get_export_data, account, request.user)
        return self.build_response(data_generator, account)

    def _get_account_name(self, account):
        return (account.name if account is not None else account.name) or ""

    def _get_account_creation(self, request, pk):
        queryset = AccountCreation.objects.all()
        visible_all_accounts = request.user.has_permission(StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS)
        if not visible_all_accounts:
            visible_accounts = request.user.get_visible_accounts_list()
            queryset = queryset.filter(account__id__in=visible_accounts)
        try:
            return queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404

    def build_response(self, data_generator, account):
        account_name = self._get_account_name(account)
        title = "Segmented report {account_name} {timestamp}".format(
            account_name=re.sub(r"\W", "-", account_name),
            timestamp=now_in_default_tz().strftime("%Y%m%d"),
        )
        user = self.request.user

        xls_report = DashboardPerformanceReport(
            custom_header=self._get_custom_header(account),
            columns_to_hide=self._get_columns_to_hide(user),
            date_format_str=self._get_date_segment_format()
        )
        return xlsx_response(title, xls_report.generate(data_generator))

    def _get_custom_header(self, account):
        header_rows = [
            "Date: {start_date} - {end_date}",
            "Group By: {metric}",
        ]
        user = self.request.user
        if user.has_permission(StaticPermissions.MANAGED_SERVICE__CAMPAIGNS_SEGMENTED):
            header_rows += [
                "Campaigns: {campaigns}",
                "Ad Groups: {ad_groups}",
            ]
        return "\n".join(header_rows) \
            .format(**self._get_header_data(account))

    def _get_header_data(self, account):
        return dict(
            metric=METRIC_REPRESENTATION.get(self._get_metric()),
            **self._get_header_data_start_end(account),
            **self._get_header_data_campaigns(account),
            **self._get_header_data_ad_groups(account),
        )

    def _get_header_data_start_end(self, account):
        start_end_aggregation = dict(
            start_date=Min("date"),
            end_date=Max("date"),
        )
        return self._get_summary_queryset(account) \
            .aggregate(**start_end_aggregation)

    def _get_header_data_campaigns(self, account):
        campaign_ids = set(self._get_summary_queryset(account).values_list("ad_group__campaign_id", flat=True))
        campaigns_names = Campaign.objects.filter(id__in=campaign_ids).order_by("name").values_list("name", flat=True)
        return dict(
            campaigns=", ".join(campaigns_names),
        )

    def _get_header_data_ad_groups(self, account):
        ad_group_ids = set(self._get_summary_queryset(account).values_list("ad_group_id", flat=True))
        ad_groups_names = AdGroup.objects.filter(id__in=ad_group_ids).order_by("name").values_list("name", flat=True)
        return dict(
            ad_groups=", ".join(ad_groups_names),
        )

    def _get_columns_to_hide(self, user):
        columns_to_hide = []
        hide_costs = not user.has_permission(StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS)
        if hide_costs:
            columns_to_hide = columns_to_hide + [DashboardPerformanceReportColumn.COST,
                                                 DashboardPerformanceReportColumn.AVERAGE_CPM,
                                                 DashboardPerformanceReportColumn.AVERAGE_CPV]
        date_segment = self._get_date_segment()
        if not date_segment:
            columns_to_hide = columns_to_hide + [DashboardPerformanceReportColumn.DATE_SEGMENT]
        show_conversions = user.has_permission(StaticPermissions.MANAGED_SERVICE__CONVERSIONS)
        if not show_conversions:
            columns_to_hide = columns_to_hide + [DashboardPerformanceReportColumn.ALL_CONVERSIONS]
        hide_delivery_data = not user.has_permission(StaticPermissions.MANAGED_SERVICE__DELIVERY)
        if hide_delivery_data:
            columns_to_hide += [
                DashboardPerformanceReportColumn.CLICKS,
                DashboardPerformanceReportColumn.IMPRESSIONS,
                DashboardPerformanceReportColumn.VIEWS,
                DashboardPerformanceReportColumn.COST,
            ]
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

    def get_export_data(self, account, user):
        filters = self.get_filters()
        data = dict(name=account.name)

        aggregation = copy(all_stats_aggregator("ad_group__campaign__"))
        for field in CLICKS_STATS:
            aggregation["sum_{}".format(field)] = Sum(field)

        show_aw_rates = user.has_permission(StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST)
        if not show_aw_rates:
            aggregation["sum_cost"] = get_client_cost_aggregation()
        stats = self._get_summary_queryset(account).aggregate(**aggregation)

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

    def _get_summary_queryset(self, account):
        filters = self.get_filters()
        fs = {"ad_group__campaign__account": account}
        if filters["start_date"]:
            fs["date__gte"] = filters["start_date"]
        if filters["end_date"]:
            fs["date__lte"] = filters["end_date"]
        if filters["ad_groups"]:
            fs["ad_group_id__in"] = filters["ad_groups"]
        elif filters["campaigns"]:
            fs["ad_group__campaign_id__in"] = filters["campaigns"]
        return AdGroupStatistic.objects.filter(**fs)

    def _get_tabs(self):
        metrics = self._get_metrics_to_display()
        return [METRIC_MAP[metric] for metric in metrics]

    def _get_metrics_to_display(self):
        metric = self._get_metric()
        if metric is not None:
            return [metric]

        metrics = list(ALL_METRICS)
        if not self.request.user.has_permission(StaticPermissions.MANAGED_SERVICE__CAMPAIGNS_SEGMENTED):
            metrics.remove(Metric.CAMPAIGN)
        if not self.request.user.has_permission(StaticPermissions.MANAGED_SERVICE__AUDIENCES):
            metrics.remove(Metric.AUDIENCE)
        return metrics

    def _validate_request_payload(self):
        self._validate_metric()
        self._validate_date_segment()

    def _validate_metric(self):
        metric = self._get_metric()
        if metric is None:
            return
        wrong_metric_error = PermissionsError("Wrong metric")
        user = self.request.user

        if metric == Metric.AUDIENCE and not user.has_permission(StaticPermissions.MANAGED_SERVICE__AUDIENCES):
            raise wrong_metric_error
        if metric == Metric.CAMPAIGN and not user.has_permission(StaticPermissions.MANAGED_SERVICE__CAMPAIGNS_SEGMENTED):
            raise wrong_metric_error

    def _validate_date_segment(self):
        date_segment = self._get_date_segment()
        if date_segment not in ALLOWED_DATE_SEGMENT:
            raise BadRequestError("Wrong date_segment")

    def _get_metric(self):
        metric_parameter = self.request.data.get("metric")
        try:
            return Metric(metric_parameter) \
                if metric_parameter \
                else None
        except ValueError as ex:
            raise BadRequestError(ex)

    def _get_date_segment(self):
        return self.request.data.get("date_segment")


class Metric(ExtendedEnum):
    AD_GROUP = "ad_group"
    AGE = "age"
    AUDIENCE = "audience"
    CAMPAIGN = "campaign"
    CHANNEL = "channel"
    CREATIVE = "creative"
    DEVICE = "device"
    GENDER = "gender"
    INTEREST = "interest"
    KEYWORD = "keyword"
    LOCATION = "location"
    OVERVIEW = "overview"
    TOPIC = "topic"
    VIDEO = "video"


ALL_METRICS = (
    Metric.OVERVIEW,
    Metric.CAMPAIGN,
    Metric.DEVICE,
    Metric.GENDER,
    Metric.AGE,
    Metric.TOPIC,
    Metric.INTEREST,
    Metric.AUDIENCE,
    Metric.KEYWORD,
    Metric.LOCATION,
    Metric.CREATIVE,
    Metric.AD_GROUP,
    Metric.CHANNEL,
    Metric.VIDEO,
)

METRIC_MAP = {
    Metric.AD_GROUP: "ad",
    Metric.AGE: "age",
    Metric.AUDIENCE: "remarketing",
    Metric.CAMPAIGN: "campaign",
    Metric.CHANNEL: "channel",
    Metric.CREATIVE: "creative",
    Metric.DEVICE: "device",
    Metric.GENDER: "gender",
    Metric.INTEREST: "interest",
    Metric.KEYWORD: "keyword",
    Metric.LOCATION: "location",
    Metric.OVERVIEW: "overview",
    Metric.TOPIC: "topic",
    Metric.VIDEO: "video",
}
DIMENSION_MAP = {value: key for key, value in METRIC_MAP.items()}
METRIC_REPRESENTATION = {
    Metric.AD_GROUP: "Ad Group",
    Metric.AGE: "Age",
    Metric.AUDIENCE: "Audience",
    Metric.CAMPAIGN: "Campaign",
    Metric.CHANNEL: "Channel",
    Metric.CREATIVE: "Creative",
    Metric.DEVICE: "Device",
    Metric.GENDER: "Gender",
    Metric.INTEREST: "Interest",
    Metric.KEYWORD: "Keyword",
    Metric.LOCATION: "Location",
    Metric.OVERVIEW: "Overview",
    Metric.TOPIC: "Topic",
    Metric.VIDEO: "Video",
}
ALLOWED_METRICS = tuple(Metric.values()) + (None,)
ALLOWED_DATE_SEGMENT = tuple(DateSegment.values()) + (None,)
DATE_SEGMENT_STRFTIME_FORMAT = {
    DateSegment.DAY.value: "%m/%d/%Y",
    DateSegment.WEEK.value: "%m/%d/%Y (W%U)",
    DateSegment.MONTH.value: "%b-%y",
    DateSegment.QUARTER.value: "%Y Q%Q",
    DateSegment.YEAR.value: "%Y",
}
