from rest_framework.serializers import ModelSerializer, \
    SerializerMethodField, Field
from aw_reporting.models import AWConnection, Account, Campaign, AdGroup, AdGroupStatistic, VideoCreativeStatistic
from django.db.models import Min, Max, Sum, Count, Case, When, Value, IntegerField as AggrIntegerField
from singledb.connector import SingleDatabaseApiConnector, SingleDatabaseApiConnectorException
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


class AccountsHeaderSerializer(ModelSerializer):
    weekly_chart = SerializerMethodField()

    start = SerializerMethodField()
    end = SerializerMethodField()
    campaigns_count = SerializerMethodField()
    ad_groups_count = SerializerMethodField()
    creative_count = SerializerMethodField()
    keywords_count = SerializerMethodField()
    videos_count = SerializerMethodField()
    channels_count = SerializerMethodField()

    status = NoneField()
    is_optimization_active = NoneField()
    is_changed = NoneField()
    goal_units = NoneField()

    @staticmethod
    def get_weekly_chart(obj):
        data = AdGroupStatistic.objects.filter(
            ad_group__campaign__account=obj
        ).values("date").order_by("-date").annotate(
            views=Sum("video_views")
        )[:7]
        chart_data = [dict(label=i['date'], value=i['views']) for i in reversed(data)]
        return chart_data

    def get_ad_groups_count(self, obj):
        return self.stats.get(obj.id, {}).get("ad_groups_count")

    def get_campaigns_count(self, obj):
        return self.stats.get(obj.id, {}).get("campaigns_count")

    def get_channels_count(self, obj):
        return self.stats.get(obj.id, {}).get("channels_count")

    def get_creative_count(self, obj):
        return self.stats.get(obj.id, {}).get("creative_count")

    def get_videos_count(self, obj):
        return self.stats.get(obj.id, {}).get("videos_count")

    def get_keywords_count(self, obj):
        return self.stats.get(obj.id, {}).get("keywords_count")

    def get_start(self, obj):
        return self.stats.get(obj.id, {}).get("min_start")

    def get_end(self, obj):
        stats = self.stats.get(obj.id)
        if stats and not stats["end_is_null"]:
            return stats["max_end"]

    def __init__(self, *args, **kwargs):
        self.stats = {}
        if args:
            if type(args[0]) is list:
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
                campaigns_count=Count("id", distinct=True),
                ad_groups_count=Count("adgroup__id", distinct=True),
                creative_count=Count("adgroup__videos_stats__creative_id", distinct=True),
                keywords_count=Count("adgroup__keywords__keyword", distinct=True),
                videos_count=Count("adgroup__managed_video_statistics__yt_id", distinct=True),
                channels_count=Count("adgroup__channel_statistics__yt_id", distinct=True),
            )
            self.stats = {i['account_id']: i for i in data}

        super(AccountsHeaderSerializer, self).__init__(*args, **kwargs)

    class Meta:
        model = Account
        fields = (
            'id', 'name', 'account_creation', 'status', 'start', 'end', 'is_optimization_active', 'is_changed',
            'creative_count', 'keywords_count', 'videos_count', 'goal_units', 'channels_count', 'campaigns_count',
            'ad_groups_count', "weekly_chart",
        )


class AccountsListSerializer(AccountsHeaderSerializer):
    creative = SerializerMethodField()
    structure = SerializerMethodField()
    goal_charts = SerializerMethodField()

    is_approved = NoneField()
    is_ended = NoneField()
    bidding_type = NoneField()
    video_ad_format = NoneField()
    delivery_method = NoneField()
    video_networks = NoneField()
    goal_type = NoneField()
    is_paused = NoneField()
    type = NoneField()

    def get_creative(self, obj):
        return self.creative.get(obj.id)

    @staticmethod
    def get_goal_charts(obj):
        charts = []
        stats = AdGroupStatistic.objects.filter(
            ad_group__campaign__account=obj
        ).values("date").order_by("date").annotate(views=Sum("video_views"))
        if stats:
            delivery_chart = dict(
                label='AW',
                value=sum(i['views'] for i in stats),
                trend=[
                    dict(label=i['date'], value=i['views'])
                    for i in stats
                ]
            )
            charts.append(delivery_chart)
        return charts

    @staticmethod
    def get_structure(obj):
        structure = [
            dict(
                id=c['id'],
                name=c['name'],
                ad_group_creations=[
                    dict(id=a['id'], name=a['name'])
                    for a in AdGroup.objects.filter(campaign_id=c['id']).values('id', 'name').order_by('name')
                ]
            )
            for c in obj.campaigns.values("id", "name").order_by("name")
        ]
        return structure

    def __init__(self, *args, **kwargs):
        self.creative = {}
        if args:
            ids = [i.id for i in args[0]]
            values = ("ad_group__campaign__account_id", "creative_id")
            data = VideoCreativeStatistic.objects.filter(
                ad_group__campaign__account_id__in=ids
            ).values(*values).order_by(*values).annotate(value=Sum("impressions"))
            creative = {}
            for c in data:
                account_id = c['ad_group__campaign__account_id']
                if account_id not in creative or creative[account_id]['value'] < c['value']:
                    creative[account_id] = dict(id=c['creative_id'], value=c['value'])

            video_ids = {i['creative_id'] for i in creative.values()}
            if video_ids:
                connector = SingleDatabaseApiConnector()
                try:
                    items = connector.get_custom_query_result(
                        model_name="video",
                        fields=["id", "title", "thumbnail_image_url"],
                        id__in=video_ids,
                        limit=len(video_ids),
                    )
                except SingleDatabaseApiConnectorException as e:
                    logger.critical(e)
                else:
                    if items:
                        items = {i['id']: i for i in items}
                        for c in creative:
                            info = items.get(c['id'], {})
                            c['name'] = info.get('title')
                            c['thumbnail'] = info.get('thumbnail_image_url')

                self.creative = creative

        super(AccountsListSerializer, self).__init__(*args, **kwargs)

    class Meta:
        model = Account
        fields = AccountsHeaderSerializer.Meta.fields + (
            'is_ended', 'is_approved', 'structure', 'bidding_type',
            'video_ad_format', 'delivery_method', 'video_networks',
            'goal_type', 'is_paused', 'type', 'goal_charts', 'creative',
        )


class AccountsDetailsSerializer(AccountsHeaderSerializer):

    def __init__(self, *args, **kwargs):

        super(AccountsDetailsSerializer, self).__init__(*args, **kwargs)

    # class Meta:
    #     model = Account
    #     fields = (
    #         'id', 'name', 'account_creation',
    #     )


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
