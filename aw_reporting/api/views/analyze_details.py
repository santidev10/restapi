import logging
from datetime import datetime, timedelta

from django.db.models import Sum, Case, When, IntegerField, ExpressionWrapper, \
    F, FloatField, Value, Max, Min, Avg
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_reporting.api.serializers import AccountsListSerializer
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import DATE_FORMAT, Account, AdGroupStatistic, \
    all_stats_aggregate, dict_norm_base_stats, dict_add_calculated_stats, \
    dict_quartiles_to_rates, GenderStatistic, Genders, AgeRangeStatistic, \
    AgeRanges, Devices, CityStatistic, BASE_STATS, CONVERSIONS, QUARTILE_STATS, VideoCreativeStatistic
from utils.db.aggregators import ConcatAggregate
from singledb.connector import SingleDatabaseApiConnector, \
    SingleDatabaseApiConnectorException
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


@demo_view_decorator
class AnalyzeDetailsApiView(APIView):
    """
    Send filters to get the account's details

    Body example:
    {}
    or
    {
        "start": "2017-05-01", "end": "2017-06-01", 
        "campaigns": ["1", "2"], "ad_groups": ["11", "12"]
    }
    """
    serializer_class = AccountsListSerializer

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

    def post(self, request, pk, **_):
        try:
            account = Account.user_objects(request.user).get(pk=pk)
        except Account.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        data = self.serializer_class(account).data  # header data
        data["details"] = self.get_details_data(account)
        data["overview"] = self.get_overview_data(account)
        return Response(data=data)

    def get_overview_data(self, account):
        filters = self.get_filters()
        fs = dict(ad_group__campaign__account=account)
        if filters["campaigns"]:
            fs["ad_group__campaign__id__in"] = filters["campaigns"]
        if filters["ad_groups"]:
            fs["ad_group__id__in"] = filters["ad_groups"]
        if filters["start_date"]:
            fs["date__gte"] = filters["start_date"]
        if filters["end_date"]:
            fs["date__lte"] = filters["end_date"]

        data = AdGroupStatistic.objects.filter(**fs).aggregate(
            **all_stats_aggregate
        )
        dict_norm_base_stats(data)
        dict_add_calculated_stats(data)
        dict_quartiles_to_rates(data)
        del data["video_impressions"]

        # "age", "gender", "device", "location"
        annotate = dict(v=Sum("cost"))
        gender = GenderStatistic.objects.filter(**fs).values(
            "gender_id").order_by("gender_id").annotate(**annotate)
        gender = [dict(name=Genders[i["gender_id"]], value=i["v"])
                  for i in gender]

        age = AgeRangeStatistic.objects.filter(**fs).values(
            "age_range_id").order_by("age_range_id").annotate(**annotate)
        age = [dict(name=AgeRanges[i["age_range_id"]], value=i["v"])
               for i in age]

        device = AdGroupStatistic.objects.filter(**fs).values(
            "device_id").order_by("device_id").annotate(**annotate)
        device = [dict(name=Devices[i["device_id"]], value=i["v"])
                  for i in device]

        location = CityStatistic.objects.filter(**fs).values(
            "city_id", "city__name").annotate(**annotate).order_by("v")[:6]
        location = [dict(name=i["city__name"], value=i["v"]) for i in location]

        data.update(gender=gender, age=age, device=device, location=location)

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
                    output_field=IntegerField()
                )
            )
            for k, sd, ed in (("this", week_start, week_end),
                              ("last", prev_week_start, prev_week_end))
            for s in BASE_STATS
        }
        weeks_stats = AdGroupStatistic.objects.filter(**fs) \
            .aggregate(**annotate)
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
                    output_field=FloatField()
                ),
                output_field=FloatField()
            ),
            ctr=ExpressionWrapper(
                Case(
                    When(
                        clicks__sum__isnull=False,
                        impressions__sum__gt=0,
                        then=F("clicks__sum") * Value(100.0) / F(
                            "impressions__sum"), ),
                    output_field=FloatField()
                ),
                output_field=FloatField()
            ),
            ctr_v=ExpressionWrapper(
                Case(
                    When(
                        clicks__sum__isnull=False,
                        video_views__sum__gt=0,
                        then=F("clicks__sum") * Value(100.0) / F(
                            "video_views__sum"),
                    ),
                    output_field=FloatField()
                ),
                output_field=FloatField()
            ),
            video_view_rate=ExpressionWrapper(
                Case(
                    When(
                        video_views__sum__isnull=False,
                        impressions__sum__gt=0,
                        then=F("video_views__sum") * Value(100.0) / F(
                            "impressions__sum"),
                    ),
                    output_field=FloatField()
                ),
                output_field=FloatField()
            ),
        )
        fields = tuple(annotate.keys())
        top_bottom_stats = AdGroupStatistic.objects.filter(**fs) \
            .values("date") \
            .order_by("date") \
            .annotate(*[Sum(s) for s in BASE_STATS]) \
            .annotate(**annotate) \
            .aggregate(
            **{"{}_{}".format(s, n): a(s)
               for s in fields
               for n, a in (("top", Max), ("bottom", Min))}
        )
        data.update(top_bottom_stats)
        return data

    @staticmethod
    def get_details_data(account):

        fs = dict(ad_group__campaign__account=account)
        data = AdGroupStatistic.objects.filter(**fs).aggregate(
            ad_network=ConcatAggregate('ad_network', distinct=True),
            average_position=Avg(
                Case(
                    When(
                        average_position__gt=0,
                        then=F('average_position'),
                    ),
                    output_field=FloatField(),
                )
            ),
            impressions=Sum("impressions"),
            **{s: Sum(s) for s in CONVERSIONS + QUARTILE_STATS}
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
                channel_info = SingleDatabaseApiConnector() \
                    .get_videos_base_info(ids)
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
        gender = [dict(name=Genders[i['gender_id']], value=i['v'])
                  for i in gender]

        age = AgeRangeStatistic.objects.filter(**fs).values(
            "age_range_id").order_by("age_range_id").annotate(**annotate)
        age = [dict(name=AgeRanges[i['age_range_id']], value=i['v'])
               for i in age]

        device = AdGroupStatistic.objects.filter(**fs).values(
            "device_id").order_by("device_id").annotate(**annotate)
        device = [dict(name=Devices[i['device_id']], value=i['v'])
                  for i in device]
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
