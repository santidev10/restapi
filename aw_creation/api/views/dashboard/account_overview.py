from datetime import datetime

from django.db.models import Case
from django.db.models import IntegerField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Sum
from django.db.models import Value
from django.db.models import When
from django.http import Http404
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AgeRanges
from aw_reporting.models import CityStatistic
from aw_reporting.models import DATE_FORMAT
from aw_reporting.models import Devices
from aw_reporting.models import GenderStatistic
from aw_reporting.models import Genders
from aw_reporting.models import OpPlacement
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import all_stats_aggregate
from aw_reporting.models import client_cost_ad_group_statistic_required_annotation
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models import dict_quartiles_to_rates
from userprofile.models import UserSettingsKey


class DashboardAccountOverviewAPIView(APIView):
    HAS_STATISTICS_KEY = "has_statistics"

    def post(self, request, pk):
        account_creation = self._get_account_creation(pk)
        data = self._get_overview_data(account_creation, request.user)
        return Response(data=data)

    def _get_account_creation(self, pk):
        account_creation_queryset = AccountCreation.objects.all()
        user_settings = self.request.user.get_aw_settings()
        if not user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS):
            visible_accounts = user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
            account_creation_queryset = account_creation_queryset.filter(account__id__in=visible_accounts)
        try:
            return account_creation_queryset.get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404

    def _get_filters(self):
        data = self.request.data
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        filters = dict(
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
            campaigns=data.get("campaigns"),
            ad_groups=data.get("ad_groups"))
        return filters

    def _get_overview_data(self, account_creation, current_user):
        filters = self._get_filters()
        fs = dict(ad_group__campaign__account=account_creation.account)
        if filters['campaigns']:
            fs["ad_group__campaign__id__in"] = filters['campaigns']
        if filters['ad_groups']:
            fs["ad_group__id__in"] = filters['ad_groups']
        if filters['start_date']:
            fs["date__gte"] = filters['start_date']
        if filters['end_date']:
            fs["date__lte"] = filters['end_date']

        queryset = AdGroupStatistic.objects.filter(**fs)
        has_statistics = queryset.exists()
        data = queryset.aggregate(**all_stats_aggregate)
        data[self.HAS_STATISTICS_KEY] = has_statistics
        dict_norm_base_stats(data)
        dict_add_calculated_stats(data)
        dict_quartiles_to_rates(data)
        del data['video_impressions']
        # 'age', 'gender', 'device', 'location'
        annotate = dict(v=Sum('cost'))
        gender = GenderStatistic.objects.filter(**fs).values(
            'gender_id').order_by('gender_id').annotate(**annotate)
        gender = [dict(name=Genders[i['gender_id']], value=i['v']) for i in
                  gender]
        age = AgeRangeStatistic.objects.filter(**fs).values(
            "age_range_id").order_by("age_range_id").annotate(**annotate)
        age = [dict(name=AgeRanges[i['age_range_id']], value=i['v']) for i in
               age]
        device = AdGroupStatistic.objects.filter(**fs).values(
            "device_id").order_by("device_id").annotate(**annotate)
        device = [dict(name=Devices[i['device_id']], value=i['v']) for i in
                  device]
        location = CityStatistic.objects.filter(**fs).values(
            "city_id", "city__name").annotate(**annotate).order_by('v')[:6]
        location = [dict(name=i['city__name'], value=i['v']) for i in location]
        data.update(gender=gender, age=age, device=device, location=location)
        self._add_chf_performance_data(data, account_creation)
        show_client_cost = not current_user.get_aw_settings() \
            .get(UserSettingsKey.DASHBOARD_AD_WORDS_RATES)
        if show_client_cost:
            data["delivered_cost"] = self._get_client_cost(fs)

        self._filter_costs(data, current_user)
        return data

    def _filter_costs(self, data, current_user):
        if current_user.get_aw_settings().get(UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN):
            hidden_values = "cost", "delivered_cost", "plan_cost", "average_cpm", "average_cpv"
            for key in hidden_values:
                data.pop(key, None)
        return data

    def _get_client_cost(self, filters):
        keys_to_extract = ("goal_type_id", "total_cost", "ordered_rate",
                           "aw_cost", "dynamic_placement", "placement_type",
                           "tech_fee", "impressions", "video_views",
                           "start", "end")
        statistics = AdGroupStatistic.objects.filter(**filters) \
            .annotate(aw_cost=Sum("cost"),
                      start=Min("ad_group__campaign__start_date"),
                      end=Max("ad_group__campaign__end_date"),
                      **client_cost_ad_group_statistic_required_annotation) \
            .values(*keys_to_extract)

        def map_data(statistic_data):
            return {key: statistic_data[key] for key in keys_to_extract}

        return sum([get_client_cost(**map_data(data)) for data in statistics])

    def _add_chf_performance_data(self, data, account_creation):
        null_fields = (
            "impressions_this_week", "cost_last_week", "impressions_last_week",
            "cost_this_week", "video_views_this_week", "clicks_this_week",
            "video_views_last_week", "clicks_last_week", "average_cpv_bottom",
            "ctr_top", "ctr_v_bottom", "ctr_bottom", "video_view_rate_top",
            "ctr_v_top", "average_cpv_top", "video_view_rate_bottom")
        for field in null_fields:
            data[field] = None
        account_campaigns_ids = account_creation.account. \
            campaigns.values_list("id", flat=True)
        filters = self._get_filters()
        campaigns_ids = filters.get("campaigns")
        ad_groups_ids = filters.get("ad_groups")
        start_date = filters.get("start_date")
        end_date = filters.get("end_date")
        placements_filters = {}
        ad_group_statistic_filters = dict(ad_group__campaign__id__in=account_campaigns_ids)
        if campaigns_ids is not None:
            placements_filters["adwords_campaigns__id__in"] = campaigns_ids
            ad_group_statistic_filters["ad_group__campaign__id__in"] = campaigns_ids
        if ad_groups_ids is not None:
            placements_filters[
                "adwords_campaigns__ad_groups__id__in"] = ad_groups_ids
            ad_group_statistic_filters["ad_group__id__in"] = ad_groups_ids
        if start_date is not None:
            ad_group_statistic_filters["date__gte"] = start_date
        if end_date is not None:
            ad_group_statistic_filters["date__lte"] = end_date
        placements_queryset = OpPlacement.objects.filter(
            adwords_campaigns__id__in=account_campaigns_ids).filter(
            **placements_filters).distinct()
        ad_group_statistic_queryset = AdGroupStatistic.objects.filter(**ad_group_statistic_filters)
        data.update(self._get_delivered_stats(ad_group_statistic_queryset))
        plan_cost = 0
        plan_impressions = 0
        plan_video_views = 0
        for placement in placements_queryset:
            plan_cost += placement.total_cost or 0
            plan_impressions += (placement.ordered_units or 0) \
                if placement.goal_type_id == SalesForceGoalType.CPM else 0
            plan_video_views += (placement.ordered_units or 0) \
                if placement.goal_type_id == SalesForceGoalType.CPV else 0
        data.update(
            {
                "plan_cost": plan_cost,
                "plan_impressions": plan_impressions,
                "plan_video_views": plan_video_views
            })

    def _get_delivered_stats(self, queryset):
        cpm_impressions_annotation = Case(When(
            ad_group__campaign__salesforce_placement__goal_type_id=SalesForceGoalType.CPM,
            then="impressions"
        ),
            output_field=IntegerField(),
            default=Value(0)
        )
        cpv_views_annotation = Case(When(
            ad_group__campaign__salesforce_placement__goal_type_id=SalesForceGoalType.CPV,
            then="video_views"
        ),
            output_field=IntegerField(),
            default=Value(0)
        )
        return queryset.annotate(
            cpm_impressions=cpm_impressions_annotation,
            cpv_video_views=cpv_views_annotation
        ).aggregate(
            delivered_cost=Sum("cost"),
            delivered_impressions=Sum("cpm_impressions"),
            delivered_video_views=Sum("cpv_video_views"),
        )
