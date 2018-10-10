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
from aw_reporting.dashboard_charts import DeliveryChart, DateSegment
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.excel_reports import DashboardPerformanceReport
from aw_reporting.excel_reports.dashboard_performance_report import DashboardPerformanceReportColumn
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import CLICKS_STATS
from aw_reporting.models import DATE_FORMAT
from aw_reporting.models import all_stats_aggregator
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models import dict_quartiles_to_rates
from userprofile.constants import UserSettingsKey
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
        return self.build_response(item.name, data_generator)

    tabs = (
        "device", "gender", "age", "topic", "interest", "remarketing",
        "keyword", "location", "creative", "ad", "channel", "video",
    )

    def build_response(self, account_name, data_generator):
        title = "{title}-analyze-{timestamp}".format(
            title=re.sub(r"\W", account_name, "-"),
            timestamp=datetime.now().strftime("%Y%m%d"),
        )
        user = self.request.user

        xls_report = DashboardPerformanceReport(
            columns_to_hide=self._get_columns_to_hide(user),
            date_format_str=self._get_date_segment_format()
        )
        return xlsx_response(title, xls_report.generate(data_generator))

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
                yield {**{"tab": dimension}, **data}

    def _get_tabs(self):
        metric = self._get_metric()
        if metric is None:
            return self.tabs
        return [METRIC_MAP[metric]]

    def _validate_request_payload(self):
        metric = self._get_metric()
        if metric not in ALLOWED_METRICS:
            raise ValueError("Wrong metric")
        date_segment = self._get_date_segment()
        if date_segment not in ALLOWED_DATE_SEGMENT:
            raise ValueError("Wrong date_segment")

    def _get_metric(self):
        return self.request.data.get("metric")

    def _get_date_segment(self):
        return self.request.data.get("date_segment")


METRIC_MAP = {
    "gender": "gender",
    "age": "age",
    "location": "location",
    "device": "device",
    "topic": "topic",
    "interest": "interest",
    "keyword": "keyword",
    "channel": "channel",
    "video": "video",
    "creative": "creative",
    "ad_group": "ad",
    "audience": "remarketing"
}
ALLOWED_METRICS = tuple(METRIC_MAP.keys()) + (None,)
ALLOWED_DATE_SEGMENT = tuple(DateSegment.values()) + (None,)
DATE_SEGMENT_STRFTIME_FORMAT = {
    DateSegment.DAY.value: "%m/%d/%Y",
    DateSegment.WEEK.value: "%w",
    DateSegment.MONTH.value: "%b-%y",
    DateSegment.YEAR.value: "%Y",
}
