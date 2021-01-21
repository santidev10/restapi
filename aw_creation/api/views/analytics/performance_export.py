import re
from copy import copy
from datetime import datetime
from functools import partial

from django.db.models import Sum
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.charts.analytics_charts import DeliveryChart
from aw_reporting.excel_reports import AnalyticsPerformanceReport
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import CLICKS_STATS
from aw_reporting.models import DATE_FORMAT
from aw_reporting.models import all_stats_aggregator
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models import dict_quartiles_to_rates
from userprofile.constants import StaticPermissions
from utils.views import xlsx_response


class AnalyticsPerformanceExportApiView(APIView):
    permission_classes = (StaticPermissions()(StaticPermissions.MANAGED_SERVICE__EXPORT),)

    def post(self, request, pk, **_):
        user = request.user
        try:
            item = AccountCreation.objects.user_related(user).get(pk=pk)
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
        columns_to_hide = []
        xls_report = AnalyticsPerformanceReport(columns_to_hide=columns_to_hide)
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

        aggregation = copy(all_stats_aggregator("ad_group__campaign__"))
        for field in CLICKS_STATS:
            aggregation["sum_{}".format(field)] = Sum(field)
        stats = AdGroupStatistic.objects.filter(**fs).aggregate(
            **aggregation
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
                show_aw_costs=True,
                **filters
            )
            items = chart.get_items()
            for data in items["items"]:
                yield {**{"tab": dimension}, **data}
