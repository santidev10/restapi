import re
from datetime import datetime
from functools import partial

from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.charts import DeliveryChart
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.excel_reports import PerformanceReport
from aw_reporting.models import dict_quartiles_to_rates, all_stats_aggregate, \
    DATE_FORMAT, AdGroupStatistic, dict_norm_base_stats, \
    dict_add_calculated_stats
from userprofile.models import UserSettingsKey
from utils.permissions import UserHasDashboardPermission
from utils.views import xlsx_response


@demo_view_decorator
class PerformanceExportApiView(APIView):
    permission_classes = (IsAuthenticated, UserHasDashboardPermission)

    def post(self, request, pk, **_):
        filters = {}
        if request.data.get("is_chf") == 1:
            user_settings = self.request.user.aw_settings
            visible_accounts = user_settings.get(
                UserSettingsKey.VISIBLE_ACCOUNTS)
            if visible_accounts:
                filters["account__id__in"] = visible_accounts
        else:
            filters["owner"] = self.request.user
        try:
            item = AccountCreation.objects.filter(**filters).get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        data_generator = partial(self.get_export_data, item)
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
        xls_report = PerformanceReport()
        return xlsx_response(title, xls_report.generate(data_generator))

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

    def get_export_data(self, item):
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

        stats = AdGroupStatistic.objects.filter(**fs).aggregate(
            **all_stats_aggregate
        )
        dict_norm_base_stats(stats)
        dict_quartiles_to_rates(stats)
        dict_add_calculated_stats(stats)
        data.update(stats)

        yield {**{"tab": "Summary"}, **data}

        accounts = []
        if account:
            accounts.append(account.id)

        for dimension in self.tabs:
            chart = DeliveryChart(
                accounts=accounts,
                dimension=dimension,
                **filters
            )
            items = chart.get_items()
            for data in items["items"]:
                yield {**{"tab": dimension}, **data}
