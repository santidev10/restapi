from datetime import datetime
from datetime import timedelta

from django.db.models import Case
from django.db.models import ExpressionWrapper
from django.db.models import F
from django.db.models import FloatField as AggrFloatField
from django.db.models import IntegerField as AggrIntegerField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Sum
from django.db.models import Value
from django.db.models import When
from django.http import Http404
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AgeRanges
from aw_reporting.models import BASE_STATS
from aw_reporting.models import CityStatistic
from aw_reporting.models import DATE_FORMAT
from aw_reporting.models import Devices
from aw_reporting.models import GenderStatistic
from aw_reporting.models import Genders
from aw_reporting.models import all_stats_aggregate
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models import dict_quartiles_to_rates
from utils.datetime import now_in_default_tz


class AnalyticsAccountOverviewAPIView(APIView):
    HAS_STATISTICS_KEY = "has_statistics"

    def post(self, request, pk):
        account_creation = self._get_account_creation(request, pk)
        data = self._get_overview_data(account_creation)
        return Response(data=data)

    def _get_account_creation(self, request, pk):
        try:
            return AccountCreation.objects.filter(owner=request.user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404

    def _get_overview_data(self, account_creation):
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
        self._add_standard_performance_data(data, fs)
        return data

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

    def _add_standard_performance_data(self, data, filters):
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
