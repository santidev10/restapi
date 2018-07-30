from datetime import timedelta, datetime

from django.db.models import Avg, Value, Case, When, ExpressionWrapper, F, \
    IntegerField as AggrIntegerField, FloatField as AggrFloatField, Sum, Min, \
    Max, IntegerField
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.api.serializers import *
from aw_creation.models import AccountCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import CONVERSIONS, QUARTILE_STATS, \
    dict_quartiles_to_rates, all_stats_aggregate, VideoCreativeStatistic, \
    GenderStatistic, Genders, AgeRangeStatistic, AgeRanges, Devices, \
    CityStatistic, BASE_STATS, DATE_FORMAT, SalesForceGoalType, OpPlacement, \
    AdGroupStatistic, dict_norm_base_stats, dict_add_calculated_stats, \
    client_cost_ad_group_statistic_required_annotation
from to_be_removed.accaount_creation_list_serializer import AccountCreationListSerializer
from userprofile.models import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.db.aggregators import ConcatAggregate
from utils.permissions import UserHasDashboardOrStaffPermission
from utils.registry import registry


@demo_view_decorator
class PerformanceAccountDetailsApiView(APIView):
    permission_classes = (IsAuthenticated, UserHasDashboardOrStaffPermission)

    HAS_STATISTICS_KEY = "has_statistics"

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
            ad_groups=data.get("ad_groups"))
        return filters

    def __obtain_account(self, request, pk):
        filters = {}
        if request.data.get("is_chf") == 1:
            user_settings = self.request.user.get_aw_settings()
            if not user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS):
                filters["account__id__in"] = \
                    user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
        else:
            filters["owner"] = self.request.user
        try:
            self.account_creation = AccountCreation.objects.filter(
                **filters).get(pk=pk)
        except AccountCreation.DoesNotExist:
            self.account_creation = None

    def post(self, request, pk, **_):
        self.__obtain_account(request, pk)
        if self.account_creation is None:
            return Response(status=HTTP_404_NOT_FOUND)
        data = AccountCreationListSerializer(
            self.account_creation, context={"request": request}).data
        show_conversions = self.request.user.get_aw_settings() \
            .get(UserSettingsKey.SHOW_CONVERSIONS)
        data["overview"] = self.get_overview_data(self.account_creation)
        data["details"] = self.get_details_data(self.account_creation,
                                                show_conversions)
        return Response(data=data)

    def get_overview_data(self, account_creation):
        filters = self.get_filters()
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
        if self.request.data.get("is_chf") == 1:
            self.add_chf_performance_data(data)
            show_client_cost = not registry.user.get_aw_settings() \
                .get(UserSettingsKey.DASHBOARD_AD_WORDS_RATES)
            if show_client_cost:
                data["delivered_cost"] = self._get_client_cost(fs)

        else:
            self.add_standard_performance_data(data, fs)
        self._filter_costs(data)
        return data

    def _filter_costs(self, data):
        user = registry.user
        if user.get_aw_settings() \
                .get(UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN) and self.request.data.get("is_chf") == 1:
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

    def add_chf_performance_data(self, data):
        null_fields = (
            "impressions_this_week", "cost_last_week", "impressions_last_week",
            "cost_this_week", "video_views_this_week", "clicks_this_week",
            "video_views_last_week", "clicks_last_week", "average_cpv_bottom",
            "ctr_top", "ctr_v_bottom", "ctr_bottom", "video_view_rate_top",
            "ctr_v_top", "average_cpv_top", "video_view_rate_bottom")
        for field in null_fields:
            data[field] = None
        account_campaigns_ids = self.account_creation.account. \
            campaigns.values_list("id", flat=True)
        filters = self.get_filters()
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

    def add_standard_performance_data(self, data, filters):
        null_fields = (
            "delivered_cost", "plan_cost", "delivered_impressions",
            "plan_impressions", "delivered_video_views", "plan_video_views")
        for field in null_fields:
            data[field] = None
        # this and last week base stats
        week_end = now_in_default_tz().date() - timedelta(days=1)
        week_start = week_end - timedelta(days=6)
        prev_week_end = week_start - timedelta(days=1)
        prev_week_start = prev_week_end - timedelta(days=6)
        annotate = {
            "{}_{}_week".format(s, k): Sum(
                Case(
                    When(
                        date__gte=sd,
                        date__lte=ed,
                        then=s,
                    ),
                    output_field=AggrIntegerField()
                )
            )
            for k, sd, ed in (("this", week_start, week_end),
                              ("last", prev_week_start, prev_week_end))
            for s in BASE_STATS}
        weeks_stats = AdGroupStatistic.objects.filter(**filters).aggregate(
            **annotate)
        data.update(weeks_stats)
        # top and bottom rates
        annotate = dict(
            average_cpv=ExpressionWrapper(
                Case(
                    When(
                        cost__sum__isnull=False,
                        video_views__sum__gt=0,
                        then=F("cost__sum") / F("video_views__sum"),
                    ),
                    output_field=AggrFloatField()
                ),
                output_field=AggrFloatField()
            ),
            ctr=ExpressionWrapper(
                Case(
                    When(
                        clicks__sum__isnull=False,
                        impressions__sum__gt=0,
                        then=F("clicks__sum") * Value(100.0) / F(
                            "impressions__sum"),
                    ),
                    output_field=AggrFloatField()
                ),
                output_field=AggrFloatField()
            ),
            ctr_v=ExpressionWrapper(
                Case(
                    When(
                        clicks__sum__isnull=False,
                        video_views__sum__gt=0,
                        then=F("clicks__sum") * Value(100.0) / F(
                            "video_views__sum"),
                    ),
                    output_field=AggrFloatField()
                ),
                output_field=AggrFloatField()
            ),
            video_view_rate=ExpressionWrapper(
                Case(
                    When(
                        video_views__sum__isnull=False,
                        impressions__sum__gt=0,
                        then=F("video_views__sum") * Value(100.0) / F(
                            "impressions__sum"),
                    ),
                    output_field=AggrFloatField()
                ),
                output_field=AggrFloatField()
            ),
        )
        fields = tuple(annotate.keys())
        top_bottom_stats = AdGroupStatistic.objects.filter(**filters).values(
            "date").order_by("date").annotate(
            *[Sum(s) for s in BASE_STATS]
        ).annotate(**annotate).aggregate(
            **{"{}_{}".format(s, n): a(s)
               for s in fields
               for n, a in (("top", Max), ("bottom", Min))})
        data.update(top_bottom_stats)

    @staticmethod
    def get_details_data(account_creation, show_conversions):
        if show_conversions:
            ads_and_placements_stats = {s: Sum(s) for s in
                                        CONVERSIONS + QUARTILE_STATS}
        else:
            ads_and_placements_stats = {s: Sum(s) for s in QUARTILE_STATS}

        fs = dict(ad_group__campaign__account=account_creation.account)
        data = AdGroupStatistic.objects.filter(**fs).aggregate(
            ad_network=ConcatAggregate('ad_network', distinct=True),
            average_position=Avg(
                Case(
                    When(
                        average_position__gt=0,
                        then=F('average_position'),
                    ),
                    output_field=AggrFloatField(),
                )
            ),
            impressions=Sum("impressions"),
            **ads_and_placements_stats
        )
        dict_quartiles_to_rates(data)
        del data['impressions']

        annotate = dict(v=Sum('cost'))
        creative = VideoCreativeStatistic.objects.filter(**fs).values(
            "creative_id").annotate(**annotate).order_by('v')[:3]
        if creative:
            ids = [i['creative_id'] for i in creative]
            creative = []
            try:
                channel_info = SingleDatabaseApiConnector().get_videos_base_info(
                    ids)
            except SingleDatabaseApiConnectorException as e:
                logger.critical(e)
            else:
                video_info = {i['id']: i for i in channel_info}
                for video_id in ids:
                    info = video_info.get(video_id, {})
                    creative.append(
                        dict(
                            id=video_id,
                            name=info.get("title"),
                            thumbnail=info.get('thumbnail_image_url'),
                        )
                    )
        data.update(creative=creative)

        # second section
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
        data.update(gender=gender, age=age, device=device)

        # third section
        charts = []
        stats = AdGroupStatistic.objects.filter(
            **fs
        ).values("date").order_by("date").annotate(
            views=Sum("video_views"),
            impressions=Sum("impressions"),
        )
        if stats:
            if any(i['views'] for i in stats):
                charts.append(
                    dict(
                        label='Views',
                        trend=[
                            dict(label=i['date'], value=i['views'])
                            for i in stats
                        ]
                    )
                )

            if any(i['impressions'] for i in stats):
                charts.append(
                    dict(
                        label='Impressions',
                        trend=[
                            dict(label=i['date'], value=i['impressions'])
                            for i in stats
                        ]
                    )
                )
        data['delivery_trend'] = charts

        return data
