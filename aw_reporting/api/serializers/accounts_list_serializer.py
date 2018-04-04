from collections import defaultdict

from django.db.models import Min, Max, Sum, Case, When, Value, \
    IntegerField as AggrIntegerField
from rest_framework.fields import SerializerMethodField
from rest_framework.serializers import ModelSerializer

from aw_reporting.api.serializers.fields import StatField
from aw_reporting.models import Campaign, Account, ConcatAggregate, \
    base_stats_aggregate, dict_norm_base_stats, dict_calculate_stats, \
    AdGroupStatistic


class AccountsListSerializer(ModelSerializer):
    # header
    account_creation = SerializerMethodField()
    weekly_chart = SerializerMethodField()
    start = SerializerMethodField()
    end = SerializerMethodField()

    impressions = StatField()
    video_views = StatField()
    cost = StatField()
    clicks = StatField()
    video_view_rate = StatField()
    ctr_v = StatField()

    status = SerializerMethodField()

    @staticmethod
    def get_account_creation(obj):
        pass

    def get_status(self, obj):
        stats = self.stats.get(obj.id)
        if stats:
            statuses = stats.get("statuses")
            if statuses:
                for s in Campaign.SERVING_STATUSES:
                    if s in statuses:
                        return s.capitalize()

    def get_weekly_chart(self, obj):
        return self.daily_chart[obj.id][-7:]

    def get_start(self, obj):
        return self.stats.get(obj.id, {}).get("min_start")

    def get_end(self, obj):
        stats = self.stats.get(obj.id)
        if stats and not stats["end_is_null"]:
            return stats["max_end"]

    def __init__(self, *args, **kwargs):
        self.stats = {}
        self.daily_chart = defaultdict(list)
        if args:
            if isinstance(args[0], Account):
                ids = [args[0].id]
            elif type(args[0]) is list:
                ids = [i.id for i in args[0]]
            else:
                ids = [args[0].id]

            data = Campaign.objects \
                .filter(account_id__in=ids) \
                .values("account_id") \
                .order_by("account_id") \
                .annotate(
                    min_start=Min("start_date"),
                    max_end=Max("end_date"),
                    end_is_null=Sum(
                        Case(
                            When(
                                end_date__isnull=True,
                                then=Value(1),
                            ),
                            default=Value(0),
                            output_field=AggrIntegerField()
                        )
                    ),
                    statuses=ConcatAggregate("status", distinct=True),
                    **base_stats_aggregate
                )
            for i in data:
                dict_norm_base_stats(i)
                dict_calculate_stats(i)
                self.stats[i["account_id"]] = i

            # data for weekly charts
            account_id_key = "ad_group__campaign__account_id"
            group_by = (account_id_key, "date")
            daily_stats = AdGroupStatistic.objects.filter(
                ad_group__campaign__account_id__in=ids
            ).values(*group_by).order_by(*group_by).annotate(
                views=Sum("video_views")
            )
            for s in daily_stats:
                self.daily_chart[s[account_id_key]].append(
                    dict(label=s["date"], value=s["views"])
                )

        super(AccountsListSerializer, self).__init__(*args, **kwargs)

    class Meta:
        model = Account
        fields = (
            "id", "name", "account_creation", "status", "start", "end",
            "clicks", "cost", "impressions", "video_views", "video_view_rate",
            "ctr_v", "weekly_chart",
        )
