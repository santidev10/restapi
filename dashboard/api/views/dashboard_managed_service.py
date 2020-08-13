from django.conf import settings
from django.db.models import BooleanField
from django.db.models import Case
from django.db.models import Q
from django.db.models import Sum
from django.db.models import When

from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response

from aw_creation.models import AccountCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.models import Campaign
from aw_reporting.models.ad_words.account import Account
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.reports.pacing_report import get_pacing_from_flights
from dashboard.api.serializers.dashboard_managed_service import DashboardManagedServiceSerializer
from dashboard.api.views.constants import DASHBOARD_MANAGED_SERVICE_CACHE_PREFIX
from dashboard.utils import get_cache_key
from rest_framework.generics import ListAPIView
from userprofile.constants import UserSettingsKey
from utils.api_paginator import CustomPageNumberPaginator
from utils.datetime import now_in_default_tz
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class DashboardManagedServicePaginator(CustomPageNumberPaginator):
    page_size = 8


class DashboardManagedServiceAPIView(ListAPIView):
    serializer_class = DashboardManagedServiceSerializer
    pagination_class = DashboardManagedServicePaginator
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_dashboard"),
            IsAdminUser
        ),
    )

    def get_queryset(self, **filters):
        """
        only Accounts that are visible to the user. If `visible all accounts` is
        set, show 'everything'.
        """
        user_settings = self.request.user.get_aw_settings()
        visibility_filter = Q() \
            if user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS) \
            else Q(account__id__in=user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS))
        queryset = AccountCreation.objects \
            .annotate(
                is_demo=Case(
                    When(account_id=DEMO_ACCOUNT_ID, then=True),
                    default=False,
                    output_field=BooleanField(),
                ),
            ) \
            .filter(
                (Q(account__managers__id__in=settings.MCC_ACCOUNT_IDS) | Q(is_demo=True))
                & Q(**filters)
                & Q(is_deleted=False)
                & visibility_filter
            ) \
            .distinct()
        return queryset.order_by("-is_demo", "is_ended", "-created_at")

    def get(self, request, *args, **kwargs):
        """
        override to modify response when account_id is passed
        """
        account_id = request.query_params.get('account_id', None)
        if account_id:
            data = self._get_extra_data(account_id)
            return Response(data=data)
        return self.list(request, *args, **kwargs)

    def _get_extra_data(self, account_id):
        """
        takes an account_id to get pacing, margin, cpv for just that account
        """
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
