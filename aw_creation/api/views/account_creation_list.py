# pylint: disable=import-error

# pylint: enable=import-error
from django.conf import settings
from django.db import transaction
from django.db.models import Case, When, Q, ExpressionWrapper, F, \
    IntegerField as AggrIntegerField, FloatField as AggrFloatField, Sum, Count, \
    Min, Max
from django.db.models.functions import Coalesce
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED

from aw_creation.api.serializers import *
from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation, default_languages
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import BASE_STATS, Account
from userprofile.models import UserSettingsKey
from utils.api_paginator import CustomPageNumberPaginator
from utils.permissions import UserHasDashboardPermission


class OptimizationAccountListPaginator(CustomPageNumberPaginator):
    page_size = 20


@demo_view_decorator
class AccountCreationListApiView(ListAPIView):
    serializer_class = AccountCreationListSerializer
    pagination_class = OptimizationAccountListPaginator
    permission_classes = (IsAuthenticated, UserHasDashboardPermission)
    annotate_sorts = dict(
        impressions=(None, Sum("account__campaigns__impressions")),
        video_views=(None, Sum("account__campaigns__video_views")),
        video_impressions=(None, Sum(Case(
            When(
                account__campaigns__video_views__gt=0,
                then="account__campaigns__impressions",
            ),
            output_field=AggrIntegerField()
        ))),
        video_clicks=(None, Sum(Case(
            When(
                account__campaigns__video_views__gt=0,
                then="account__campaigns__clicks",
            ),
            output_field=AggrIntegerField()
        ))),
        clicks=(None, Sum("account__campaigns__clicks")),
        cost=(None, Sum("account__campaigns__cost")),
        video_view_rate=(
            ('video_views', 'video_impressions'), ExpressionWrapper(
                Case(
                    When(
                        video_views__isnull=False,
                        video_impressions__gt=0,
                        then=F("video_views") * 1.0 / F("video_impressions"),
                    ),
                    output_field=AggrFloatField()
                ),
                output_field=AggrFloatField()
            )),
        ctr_v=(("video_clicks", "video_views"), ExpressionWrapper(
            Case(
                When(
                    video_clicks__isnull=False,
                    video_views__gt=0,
                    then=F("video_clicks") * 1.0 / F("video_views"),
                ),
                output_field=AggrFloatField()
            ),
            output_field=AggrFloatField()
        )),
        name=(
            None,
            Case(
                When(
                    is_managed=True,
                    then=F("name")
                ),
                default=F("account__name"),
            )
        ),
    )

    def get(self, request, *args, **kwargs):
        # import "read only" accounts:
        # user has access to them,
        # but they are not connected to his account creations
        # page: Dashboard
        if request.query_params.get("is_chf") == "1":
            user_settings = self.request.user.get_aw_settings()
            if not user_settings[UserSettingsKey.VISIBLE_ALL_ACCOUNTS]:
                visible_account_ids = self.request.user.get_aw_settings()\
                                          .get(UserSettingsKey.VISIBLE_ACCOUNTS)
                read_accounts = Account.objects.filter(
                    id__in=visible_account_ids).exclude(
                    account_creations__owner=request.user).values("id", "name")
            else:
                read_accounts = Account.objects.exclude(
                    account_creations__owner=request.user).values("id", "name")
        # page: Media Buying / Analytics
        else:
            read_accounts = Account.user_objects(self.request.user)\
                .filter(can_manage_clients=False)\
                .exclude(account_creations__owner=request.user)\
                .values("id", "name")

        bulk_create = [
            AccountCreation(
                account_id=i['id'],
                name="",
                owner=request.user,
                is_managed=False,
            )
            for i in read_accounts
        ]
        if bulk_create:
            AccountCreation.objects.bulk_create(bulk_create)
        response = super(AccountCreationListApiView, self).get(
            request, *args, **kwargs)
        return response

    def get_queryset(self, **filters):
        filters["owner"] = self.request.user
        filters["is_deleted"] = False
        if self.request.query_params.get("is_chf") == "1":
            user_settings = self.request.user.get_aw_settings()
            if not user_settings[UserSettingsKey.VISIBLE_ALL_ACCOUNTS]:
                filters["account__id__in"] = user_settings.get(
                    UserSettingsKey.VISIBLE_ACCOUNTS)
            chf_account_id = settings.CHANNEL_FACTORY_ACCOUNT_ID
            filters["account__managers__id"] = chf_account_id
        else:
            filters["account__in"] = Account.user_objects(self.request.user)
        queryset = AccountCreation.objects.filter(**filters)

        sort_by = self.request.query_params.get("sort_by")
        if sort_by in self.annotate_sorts:
            dependencies, annotate = self.annotate_sorts[sort_by]
            if dependencies:
                queryset = queryset.annotate(
                    **{d: self.annotate_sorts[d][1] for d in dependencies})
            if sort_by == "name":
                sort_by = "sort_by"
            else:
                annotate = Coalesce(annotate, 0)
                sort_by = "-sort_by"
            queryset = queryset.annotate(sort_by=annotate)
        else:
            sort_by = "-created_at"
        return queryset.order_by("is_ended", sort_by)

    def filter_queryset(self, queryset):
        filters = self.request.query_params

        search = filters.get("search")
        if search:
            queryset = queryset.filter(Q(name__icontains=search) |
                                       (Q(is_managed=False) & Q(
                                           account__name__icontains=search)))

        min_campaigns_count = filters.get("min_campaigns_count")
        max_campaigns_count = filters.get("max_campaigns_count")
        if min_campaigns_count or max_campaigns_count:
            queryset = queryset.annotate(
                campaign_creations_count=Count("campaign_creations",
                                               distinct=True))

            queryset = queryset.annotate(campaigns_count=Case(
                When(
                    campaign_creations_count=0,
                    then=Count("account__campaigns", distinct=True),
                ),
                default="campaign_creations_count",
                output_field=AggrIntegerField(),
            ),
            )

            if min_campaigns_count:
                queryset = queryset.filter(
                    campaigns_count__gte=min_campaigns_count)
            if max_campaigns_count:
                queryset = queryset.filter(
                    campaigns_count__lte=max_campaigns_count)

        min_start = filters.get("min_start")
        max_start = filters.get("max_start")
        if min_start or max_start:
            queryset = queryset.annotate(
                start=Coalesce(Min("campaign_creations__start"),
                               Min("account__campaigns__start_date")))
            if min_start:
                queryset = queryset.filter(start__gte=min_start)
            if max_start:
                queryset = queryset.filter(start__lte=max_start)

        min_end = filters.get("min_end")
        max_end = filters.get("max_end")
        if min_end or max_end:
            queryset = queryset.annotate(
                end=Coalesce(Max("campaign_creations__end"),
                             Max("account__campaigns__end_date")))
            if min_end:
                queryset = queryset.filter(end__gte=min_end)
            if max_end:
                queryset = queryset.filter(end__lte=max_end)
        status = filters.get("status")
        if status:
            if status == "Ended":
                queryset = queryset.filter(is_ended=True, is_managed=True)
            elif status == "Paused":
                queryset = queryset.filter(is_paused=True, is_managed=True,
                                           is_ended=False)
            elif status == "Running":
                running_filter = Q(sync_at__isnull=False, is_managed=True,
                                   is_paused=False, is_ended=False) | \
                                 Q(is_managed=False)
                queryset = queryset.filter(running_filter)
            elif status == "Approved":
                queryset = queryset.filter(is_approved=True, is_managed=True,
                                           sync_at__isnull=True,
                                           is_paused=False, is_ended=False)
            elif status == "Pending":
                queryset = queryset.filter(is_approved=False, is_managed=True,
                                           sync_at__isnull=True,
                                           is_paused=False, is_ended=False)

        if "from_aw" in filters:
            from_aw = filters.get('from_aw') == '1'
            queryset = queryset.filter(is_managed=not from_aw)

        annotates = {}
        second_annotates = {}
        having = {}
        for metric in (
                "impressions", "video_views", "clicks", "cost",
                "video_view_rate",
                "ctr_v"):
            for is_max, option in enumerate(("min", "max")):
                filter_value = filters.get("{}_{}".format(option, metric))
                if filter_value:
                    if metric in BASE_STATS:
                        annotate_key = "sum_{}".format(metric)
                        annotates[annotate_key] = Sum(
                            "account__campaigns__{}".format(metric))
                        having["{}__{}".format(
                            annotate_key, "lte" if is_max else "gte")
                        ] = filter_value
                    elif metric == "video_view_rate":
                        annotates['video_impressions'] = Sum(
                            Case(
                                When(
                                    account__campaigns__video_views__gt=0,
                                    then="account__campaigns__impressions",
                                ),
                                output_field=AggrIntegerField()
                            )
                        )
                        annotates['sum_video_views'] = Sum(
                            "account__campaigns__video_views")
                        second_annotates[metric] = Case(
                            When(
                                sum_video_views__isnull=False,
                                video_impressions__gt=0,
                                then=F("sum_video_views") * 100. / F(
                                    "video_impressions"),
                            ),
                            output_field=AggrFloatField()
                        )
                        having["{}__{}".format(
                            metric, "lte" if is_max else "gte")] = filter_value
                    elif metric == "ctr_v":
                        annotates['video_clicks'] = Sum(
                            Case(
                                When(
                                    account__campaigns__video_views__gt=0,
                                    then="account__campaigns__clicks",
                                ),
                                output_field=AggrIntegerField()
                            )
                        )
                        annotates['sum_video_views'] = Sum(
                            "account__campaigns__video_views")
                        second_annotates[metric] = Case(
                            When(
                                video_clicks__isnull=False,
                                sum_video_views__gt=0,
                                then=F("video_clicks") * 100. / F(
                                    "sum_video_views"),
                            ),
                            output_field=AggrFloatField()
                        )
                        having["{}__{}".format(
                            metric, "lte" if is_max else "gte")] = filter_value
        if annotates:
            queryset = queryset.annotate(**annotates)
        if second_annotates:
            queryset = queryset.annotate(**second_annotates)
        if having:
            queryset = queryset.filter(**having)

        return queryset

    def post(self, *a, **_):
        account_count = AccountCreation.objects.filter(
            owner=self.request.user).count()

        with transaction.atomic():
            account_creation = AccountCreation.objects.create(
                name="Account {}".format(account_count + 1),
                owner=self.request.user,
            )
            campaign_creation = CampaignCreation.objects.create(
                name="Campaign 1",
                account_creation=account_creation,
            )
            ad_group_creation = AdGroupCreation.objects.create(
                name="AdGroup 1",
                campaign_creation=campaign_creation,
            )
            AdCreation.objects.create(
                name="Ad 1",
                ad_group_creation=ad_group_creation,
            )
            AccountCreation.objects.filter(id=account_creation.id).update(
                is_deleted=True)  # do not show it in the list

        for language in default_languages():
            campaign_creation.languages.add(language)

        data = AccountCreationSetupSerializer(account_creation).data
        return Response(status=HTTP_202_ACCEPTED, data=data)
