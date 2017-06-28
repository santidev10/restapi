from rest_framework.serializers import ModelSerializer, SerializerMethodField
from aw_reporting.models import AWConnection, Account, Campaign, AdGroup, AdGroupStatistic, ConcatAggregate, \
    dict_norm_base_stats, base_stats_aggregate, dict_calculate_stats
from django.db.models import Min, Max, Sum, Count, Case, When, Value, IntegerField as AggrIntegerField
import logging

logger = logging.getLogger(__name__)


class MCCAccountSerializer(ModelSerializer):
    class Meta:
        model = Account
        fields = ("id", "name", "currency_code", "timezone")


class AWAccountConnectionSerializer(ModelSerializer):
    mcc_accounts = SerializerMethodField()

    @staticmethod
    def get_mcc_accounts(obj):
        qs = Account.objects.filter(
            mcc_permissions__aw_connection=obj).order_by("name")
        return MCCAccountSerializer(qs, many=True).data

    class Meta:
        model = AWConnection
        fields = ("email", "mcc_accounts")


class NoneField(SerializerMethodField):
    def to_representation(self, value):
        return


class StatField(SerializerMethodField):
    def to_representation(self, value):
        return self.parent.stats.get(value.id, {}).get(self.field_name)


class AccountsListSerializer(ModelSerializer):
    # header
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

    def get_status(self, obj):
        stats = self.stats.get(obj.id)
        if stats:
            statuses = stats.get("statuses")
            if statuses:
                for s in Campaign.SERVING_STATUSES:
                    if s in statuses:
                        return s.capitalize()

    @staticmethod
    def get_weekly_chart(obj):
        data = AdGroupStatistic.objects.filter(
            ad_group__campaign__account=obj
        ).values("date").order_by("-date").annotate(
            views=Sum("video_views")
        )[:7]
        chart_data = [dict(label=i['date'], value=i['views']) for i in reversed(data)]
        return chart_data

    def get_start(self, obj):
        return self.stats.get(obj.id, {}).get("min_start")

    def get_end(self, obj):
        stats = self.stats.get(obj.id)
        if stats and not stats["end_is_null"]:
            return stats["max_end"]

    def __init__(self, *args, **kwargs):
        self.stats = {}
        if args:
            if isinstance(args[0], Account):
                ids = [args[0].id]
            elif type(args[0]) is list:
                ids = [i.id for i in args[0]]
            else:
                ids = [args[0].id]

            data = Campaign.objects.filter(
                account_id__in=ids
            ).values('account_id').order_by('account_id').annotate(
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
                **base_stats_aggregate,
            )
            for i in data:
                dict_norm_base_stats(i)
                dict_calculate_stats(i)
                self.stats[i['account_id']] = i

        super(AccountsListSerializer, self).__init__(*args, **kwargs)

    class Meta:
        model = Account
        fields = (
            'id', 'name', 'account_creation', 'status', 'start', 'end',
            'clicks', 'cost', 'impressions', 'video_views', 'video_view_rate', 'ctr_v',
            "weekly_chart",
        )


class AdGroupListSerializer(ModelSerializer):

    class Meta:
        model = AdGroup
        fields = ('id', 'name', 'status')


class CampaignListSerializer(ModelSerializer):

    ad_groups = AdGroupListSerializer(source="adgroup_set", many=True)

    class Meta:
        model = Campaign
        fields = (
            'id', 'name', 'ad_groups', 'status', 'start_date', 'end_date',
        )
