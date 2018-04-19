from datetime import datetime

import pytz
from django.conf import settings
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated

from aw_reporting.models import Opportunity
from aw_reporting.tools.health_check_tool import HealthCheckTool
from utils.api_paginator import CustomPageNumberPaginator


class HealthCheckPaginator(CustomPageNumberPaginator):
    page_size = 10


class HealthCheckView(ListAPIView):
    permission_classes = (IsAuthenticated, )
    paginator = HealthCheckPaginator()

    def get_queryset(self):
        queryset = Opportunity.objects.filter(
            probability=100
        ).select_related("ad_ops_manager", "ad_ops_qa_manager",
                         "account_manager").order_by("name", "-start")
        return queryset

    def filter_queryset(self, queryset):
        request_data = self.request.GET
        search = request_data.get("search")
        if search:
            queryset = queryset.filter(name__icontains=search)
        period = request_data.get("period")
        if period:
            today = datetime.now(
                tz=pytz.timezone(settings.DEFAULT_TIMEZONE)).date()
            if period == "current":
                queryset = queryset.filter(start__lte=today, end__gte=today)
            elif period == "past":
                queryset = queryset.filter(end__lt=today)
            elif period == "future":
                queryset = queryset.filter(start__gt=today)
        multi_del = ","
        ad_ops = request_data.get("ad_ops")
        if ad_ops:
            queryset = queryset.filter(
                ad_ops_manager_id__in=ad_ops.split(multi_del))
        am = request_data.get("am")
        if am:
            queryset = queryset.filter(
                account_manager_id__in=am.split(multi_del))
        account = request_data.get("account")
        if account:
            queryset = queryset.filter(account_id__in=account.split(multi_del))
        start = request_data.get("start")
        if start:
            start = datetime.strptime(start, "%Y-%m-%d").date()
            queryset = queryset.exclude(start__lt=start).exclude(
                end__lt=start)
        end = request_data.get("end")
        if end:
            end = datetime.strptime(end, "%Y-%m-%d").date()
            queryset = queryset.exclude(end__gt=end).exclude(
                start__gt=end)
        brands = request_data.get("brands")
        if brands is not None:
            queryset = queryset.filter(brand__in=brands.split(","))
        sales = request_data.get("sales_rep")
        if sales is not None:
            queryset = queryset.filter(sales_manager_id__in=sales.split(","))
        return queryset

    def get(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        response = HealthCheckTool(page)
        return self.get_paginated_response(response)
