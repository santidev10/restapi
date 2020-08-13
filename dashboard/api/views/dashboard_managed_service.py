from datetime import timedelta
from numbers import Number

from django.conf import settings
from django.db.models import ExpressionWrapper, Q
from django.db.models import BooleanField
from django.db.models import F
from django.db.models import Sum
from django.db.models import Case
from django.db.models import When
from django.db.models import FloatField
from django.db.models import IntegerField

from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.api.serializers import DashboardAccountCreationListSerializer
from aw_creation.api.views.analytics.account_creation_list import OptimizationAccountListPaginator
from aw_creation.models import AccountCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.reports.pacing_report import get_pacing_from_flights
from aw_reporting.models import Campaign
from cache.models import CacheItem
from dashboard.api.serializers.dashboard_managed_service import DashboardManagedServiceAveragesAdminSerializer
from dashboard.api.serializers.dashboard_managed_service import DashboardManagedServiceAveragesSerializer
from dashboard.api.serializers.dashboard_managed_service import DashboardManagedServiceOpportunityAdminSerializer
from dashboard.api.serializers.dashboard_managed_service import DashboardManagedServiceOpportunitySerializer
from dashboard.api.views.constants import DASHBOARD_MANAGED_SERVICE_CACHE_PREFIX
from dashboard.utils import get_cache_key
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from aw_reporting.models.ad_words.account import Account
from rest_framework.generics import ListAPIView
from utils.api_paginator import CustomPageNumberPaginator


class DashboardManagedServicePaginator(CustomPageNumberPaginator):
    page_size = 8


class DashboardManagedServiceAPIView(ListAPIView):
    serializer_class = DashboardAccountCreationListSerializer
    pagination_class = OptimizationAccountListPaginator
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_dashboard"),
            IsAdminUser
        ),
    )

    FIELDS = ['video_view_rate', 'ctr', 'video_quartile_100_rate', 'margin',
              'pacing', 'cpv']

    CACHE_TTL = 60 * 30

    def get_queryset(self, **filters):
        user_settings = self.request.user.get_aw_settings()
        visibility_filter = Q() \
            if user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS) \
            else Q(account__id__in=user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS))
        queryset = AccountCreation.objects.all() \
            .annotate(is_demo=Case(When(account_id=DEMO_ACCOUNT_ID, then=True),
                                   default=False,
                                   output_field=BooleanField(), ), ) \
            .filter((Q(account__managers__id__in=settings.MCC_ACCOUNT_IDS) | Q(is_demo=True)) & Q(**filters)
                    & Q(is_deleted=False)
                    & visibility_filter)
        return queryset.order_by("-is_demo", "is_ended", "-created_at")

    # def get(self, request, *args, **kwargs):
    #     cache_key = self.get_cache_key(request.user.id)
    #     now = now_in_default_tz()
    #     try:
    #         raise CacheItem.DoesNotExist
    #         cache = CacheItem.objects.get(key=cache_key)
    #         data = cache.value
    #         if cache.created_at < now - timedelta(seconds=self.CACHE_TTL):
    #             data = self._get_data()
    #             cache.value = data
    #             cache.created_at = now
    #             cache.save()
    #     except CacheItem.DoesNotExist:
    #         data = self._get_data()
    #         # CacheItem.objects.create(key=cache_key, value=data)
    #     return Response(data=data)

    def _get_data(self):
        aw_settings = self.request.user.get_aw_settings()
        all_visible = aw_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS, False)
        if not all_visible:
            visible_account_ids = aw_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS, [])
            accounts = self.get_visible_accounts(visible_account_ids)
        # opportunities = PacingReport().get_opportunities(get={}, user=self.request.user,
        #                                                  aw_cid=visible_account_ids,
        #                                                  managed_service_data=True)
        # averages_serializer_class, opportunity_serializer_class = self.get_serializer_classes()
        # return {
        #     'averages': averages_serializer_class(self.get_averages(opportunities)).data,
        #     'items': opportunity_serializer_class(opportunities, many=True).data,
        # }

    def get_visible_accounts(self, visible_account_ids: list=[]):
        # TODO build query to get all accounts
        if not visible_account_ids:
            pass
        query = Account.objects.filter(id__in=visible_account_ids) \
            .annotate(**self.ANNOTATIONS)
        for account in query:
            print(account)

    def get_serializer_class(self):
        """return different serializer depending on user perms"""
        if self.request.user.is_staff:
            return DashboardManagedServiceOpportunityAdminSerializer
        return DashboardManagedServiceOpportunitySerializer

    def get_averages(self, opportunities):
        """
        compute mean averages for select Opportunity Fields
        """
        values = {}
        # TODO: add viewability/viewable_rate to default fields after VIQ2-428
        fields = self.FIELDS
        for opportunity in opportunities:
            for field_name in fields:
                field_values = values.get(field_name, [])
                field_values.append(opportunity.get(field_name, 0))
                values[field_name] = field_values
        averages = {}
        for field_name in fields:
            field_values = values.get(field_name, [])
            filtered = [value for value in field_values if isinstance(value, Number)]
            averages[field_name] = sum(filtered) / len(field_values) \
                if len(field_values) and sum(filtered) \
                else None
        return averages

    @staticmethod
    def get_cache_key(user_id):
        cache_key = get_cache_key(user_id, prefix=DASHBOARD_MANAGED_SERVICE_CACHE_PREFIX)
        return cache_key

    def _get_extra_data(self, request):
        account_id = request.query_params["account_id"]
        account = Account.objects.get(id=account_id)
        report = PacingReport()
        today = now_in_default_tz().date()

        flights = report.get_flights_data(placement__opportunity__aw_cid__contains=account.id)
        plan_cost = sum(f["total_cost"] for f in flights if f["start"] <= today)
        actual_cost = Campaign.objects.filter(account=account).aggregate(Sum("cost"))["cost__sum"]
        delivery_stats = report.get_delivery_stats_from_flights(flights)

        pacing = get_pacing_from_flights(flights)
        margin = report.get_margin_from_flights(flights, actual_cost, plan_cost)
        cpv = delivery_stats["cpv"]
        extra_data = {
            "pacing": pacing,
            "margin": margin,
            "cpv": cpv
        }
        return extra_data
