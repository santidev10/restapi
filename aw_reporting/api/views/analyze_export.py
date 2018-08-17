import csv
import re
from datetime import datetime
from io import StringIO

from django.db.models import Sum
from django.http import StreamingHttpResponse
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_reporting.charts import DeliveryChart
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import Account
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import BASE_STATS
from aw_reporting.models import DATE_FORMAT
from aw_reporting.models import QUARTILE_STATS
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_quartiles_to_rates


@demo_view_decorator
class AnalyzeExportApiView(APIView):
    """
    Send filters to download a csv report

    Body example:

    {"campaigns": ["1", "2"]}
    """

    def post(self, request, pk, **_):
        try:
            item = Account.user_objects(request.user).get(pk=pk)
        except Account.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        def data_generator():
            return self.get_export_data(item)

        return self.stream_response(item.name, data_generator)

    file_name = "{title}-analyze-{timestamp}.csv"

    column_names = (
        "", "Name", "Impressions", "Views", "Cost", "Average cpm",
        "Average cpv", "Clicks", "Ctr(i)", "Ctr(v)", "View rate",
        "25%", "50%", "75%", "100%",
    )
    column_keys = (
        "name", "impressions", "video_views", "cost", "average_cpm",
        "average_cpv", "clicks", "ctr", "ctr_v", "video_view_rate",
        "video25rate", "video50rate", "video75rate", "video100rate",
    )
    tabs = (
        "device", "gender", "age", "topic", "interest", "remarketing",
        "keyword", "location", "creative", "ad", "channel", "video",
    )

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

    @staticmethod
    def stream_response_generator(data_generator):
        for row in data_generator():
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(row)
            yield output.getvalue()

    def stream_response(self, item_name, generator):
        generator = self.stream_response_generator(generator)
        response = StreamingHttpResponse(generator,
                                         content_type="text/csv")
        filename = self.file_name.format(
            title=re.sub(r"\W", item_name, "-"),
            timestamp=datetime.now().strftime("%Y%m%d"),
        )
        response["Content-Disposition"] = "attachment; " \
                                          "filename=\"{}\"".format(filename)
        return response

    def get_export_data(self, item):
        filters = self.get_filters()

        data = dict(name=item.name)

        fs = {"ad_group__campaign__account_id": item.id}
        if filters["start_date"]:
            fs["date__gte"] = filters["start_date"]
        if filters["end_date"]:
            fs["date__lte"] = filters["end_date"]
        if filters["ad_groups"]:
            fs["ad_group_id__in"] = filters["ad_groups"]
        elif filters["campaigns"]:
            fs["ad_group__campaign_id__in"] = filters["campaigns"]

        stats = AdGroupStatistic.objects.filter(**fs).aggregate(
            **{s: Sum(s) for s in BASE_STATS + QUARTILE_STATS}
        )
        dict_quartiles_to_rates(stats)
        dict_add_calculated_stats(stats)
        data.update(stats)

        yield self.column_names
        yield ["Summary"] + [data.get(n) for n in self.column_keys]

        for dimension in self.tabs:
            chart = DeliveryChart(
                accounts=[item.id],
                dimension=dimension,
                **filters
            )
            items = chart.get_items()
            for data in items["items"]:
                yield [dimension.capitalize()] + \
                      [data[n] for n in self.column_keys]
