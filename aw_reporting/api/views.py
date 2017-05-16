from django.http import HttpResponse
from django.http import StreamingHttpResponse
from django.utils import timezone
from rest_framework.generics import ListAPIView, RetrieveAPIView, UpdateAPIView
from rest_framework.status import HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST, \
    HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_403_FORBIDDEN, HTTP_405_METHOD_NOT_ALLOWED
from rest_framework.views import APIView
from aw_reporting.api.serializers import *
from aw_reporting.demo import demo_view_decorator
from datetime import datetime
from io import StringIO
import logging
import csv
import re

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%d"


@demo_view_decorator
class AnalyzeAccountsListApiView(ListAPIView):

    def list(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


@demo_view_decorator
class AnalyzeAccountCampaignsListApiView(APIView):

    def get(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


@demo_view_decorator
class AnalyzeDetailsApiView(APIView):

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

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


@demo_view_decorator
class AnalyzeChartApiView(APIView):

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
            indicator=data.get("indicator", "average_cpv"),
            dimension=data.get("dimension"),
        )
        return filters

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


@demo_view_decorator
class AnalyzeChartItemsApiView(APIView):

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
            segmented=data.get("segmented"),
        )
        return filters

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


@demo_view_decorator
class AnalyzeExportApiView(APIView):

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")

    file_name = "{title}-analyze-{timestamp}.csv"

    column_names = (
        "", "Name",  "Impressions", "Views",  "Cost", "Average cpm",
        "Average cpv", "Clicks", "Ctr(i)", "Ctr(v)", "View rate",
        "25%", "50%", "75%", "100%",
    )
    column_keys = (
        'name', 'impressions', 'video_views', 'cost', 'average_cpm',
        'average_cpv', 'clicks', 'ctr', 'ctr_v', 'video_view_rate',
        'video25rate', 'video50rate', 'video75rate', 'video100rate',
    )
    tabs = (
        'device', 'gender', 'age', 'topic', 'interest', 'remarketing',
        'keyword', 'location', 'creative', 'ad', 'channel', 'video',
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
            campaigns=data.get('campaigns'),
            ad_groups=data.get('ad_groups'),
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
                                         content_type='text/csv')
        filename = self.file_name.format(
            title=re.sub(r"\W", item_name, '-'),
            timestamp=datetime.now().strftime("%Y%m%d"),
        )
        response['Content-Disposition'] = 'attachment; ' \
                                          'filename="{}"'.format(filename)
        return response


@demo_view_decorator
class AnalyzeExportWeeklyReport(APIView):

    def get_filters(self):
        data = self.request.data
        filters = dict(
            campaigns=data.get('campaigns'),
            ad_groups=data.get('ad_groups'),
        )
        return filters

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


class TrackApiBase(APIView):
    indicators = (
        ('average_cpv', 'CPV'),
        ('average_cpm', 'CPM'),
        ('video_view_rate', 'View Rate'),
        ('ctr', 'CTR(i)'),
        ('ctr_v', 'CTR(v)'),
        ('impressions', 'Impressions'),
        ('video_views', 'Views'),
        ('clicks', 'Clicks'),
        ('cost', 'Costs'),
    )
    breakdowns = (
        ("daily", "Daily"),
        ("hourly", "Hourly"),
    )
    dimensions = (
        ("creative", "Creatives"),
        ("device", "Devices"),
        ("age", "Ages"),
        ("gender", "Genders"),
        ("video", "Top videos"),
        ("channel", "Top channels"),
        ("interest", "Top interests"),
        ("topic", "Top topics"),
        ("keyword", "Top keywords"),
    )

    def get_filters(self):
        data = self.request.query_params
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        filters = dict(
            account=data.get('account'),
            campaign=data.get('campaign'),
            indicator=data.get('indicator', self.indicators[0][0]),
            breakdown=data.get('breakdown'),
            dimension=data.get('dimension'),
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
        )
        return filters


@demo_view_decorator
class TrackFiltersListApiView(TrackApiBase):

    def get(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


@demo_view_decorator
class TrackChartApiView(TrackApiBase):

    def get(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


@demo_view_decorator
class TrackAccountsDataApiView(TrackApiBase):
    def get(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


