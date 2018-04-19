from datetime import datetime

import pytz
from django.conf import settings
from django.db.models import Min, Max
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.models import Opportunity
from aw_reporting.tools.health_check_tool import HealthCheckTool
from utils.api_paginator import CustomPageNumberPaginator


class HealthCheckPaginator(CustomPageNumberPaginator):
    page_size = 10


class HealthCheckApiView(ListAPIView):
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


class HealthCheckFiltersApiView(APIView):
    permission_classes = (IsAuthenticated, )

    def get(self, request):
        queryset = Opportunity.objects.all()
        group_by = ("account_manager__id", "account_manager__name")
        am_users = queryset.filter(account_manager__is_active=True,
                                   account_manager__isnull=False) \
            .values(*group_by) \
            .order_by(*group_by).distinct()
        group_by = ("ad_ops_manager__id", "ad_ops_manager__name")
        ad_ops_users = queryset.filter(ad_ops_manager__is_active=True,
                                       ad_ops_manager__isnull=False) \
            .values(*group_by) \
            .order_by(*group_by) \
            .distinct()
        group_by = ("account__id", "account__name")
        accounts = queryset.filter(account__isnull=False).values(
            *group_by).order_by(*group_by).distinct()
        date_range = queryset.aggregate(Min('start'), Max('end'))
        group_by = ("sales_manager__id", "sales_manager__name")
        sales_rep = queryset.filter(sales_manager__is_active=True,
                                    sales_manager__isnull=False) \
            .values(*group_by) \
            .order_by(*group_by) \
            .distinct()
        brands = queryset.filter(brand__isnull=False).values(
            'brand').order_by('brand').distinct()
        filters = dict(
            period=[
                dict(id=status, name=status.capitalize())
                for status in ("current", "past", "future")
            ],
            ad_ops=[
                dict(id=u['ad_ops_manager__id'],
                     name=u['ad_ops_manager__name'])
                for u in ad_ops_users
            ],
            am=[
                dict(id=u['account_manager__id'],
                     name=u['account_manager__name'])
                for u in am_users
            ],
            account=[
                dict(id=u['account__id'], name=u['account__name'])
                for u in accounts
            ],
            date_range=dict(min=date_range['start__min'],
                            max=date_range['end__max']),
            sales_rep=[
                dict(id=u['sales_manager__id'], name=u['sales_manager__name'])
                for u in sales_rep],
            brands=[b['brand'] for b in brands]
        )
        return Response(filters)
