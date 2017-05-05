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
import logging

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
            indicator=data.get("indicator"),
        )
        return filters

    def post(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


class AnalyzeChartItemsApiView(ListAPIView):

    def list(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


class AnalyzeExportApiView(ListAPIView):

    def list(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")


class AnalyzeExportWeeklyReport(ListAPIView):

    def list(self, request, *args, **kwargs):
        raise NotImplementedError("Vzhukh!")

