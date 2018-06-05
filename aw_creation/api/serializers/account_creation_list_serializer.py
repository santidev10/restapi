import logging
from collections import defaultdict

from django.db.models import Min, Max, Sum, Count
from rest_framework.serializers import ModelSerializer, SerializerMethodField, \
    BooleanField

from aw_creation.models import CampaignCreation, AccountCreation
from aw_reporting.api.serializers.fields import StatField
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.models import AdGroupStatistic, \
    Campaign, dict_norm_base_stats, \
    ConcatAggregate, VideoCreativeStatistic, Ad, \
    Opportunity, dict_add_calculated_stats, base_stats_aggregator, \
    client_cost_campaign_required_annotation, F
from aw_reporting.utils import safe_max
from userprofile.models import UserSettingsKey
from utils.registry import registry
from utils.serializers import ExcludeFieldsMixin

logger = logging.getLogger(__name__)


class StruckField(SerializerMethodField):
    def to_representation(self, value):
        return self.parent.struck.get(value.id, {}).get(self.field_name)


class AccountCreationListSerializer(ModelSerializer, ExcludeFieldsMixin):
    ACCOUNT_ID_KEY = "account__account_creations__id"
    is_changed = BooleanField()
    name = SerializerMethodField()
    thumbnail = SerializerMethodField()
    weekly_chart = SerializerMethodField()
    start = SerializerMethodField()
    end = SerializerMethodField()
    is_disapproved = SerializerMethodField()
    from_aw = SerializerMethodField()
    status = SerializerMethodField()
    cost_method = SerializerMethodField()
    updated_at = SerializerMethodField()
    # analytic data
    impressions = StatField()
    video_views = StatField()
    cost = StatField()
    clicks = StatField()
    video_view_rate = StatField()
    # structural data
    ad_count = StruckField()
    channel_count = StruckField()
    video_count = StruckField()
    interest_count = StruckField()
    topic_count = StruckField()
    keyword_count = StruckField()
    # opportunity data
    brand = SerializerMethodField()
    agency = SerializerMethodField()

    average_cpv = StatField()
    average_cpm = StatField()

    ctr = StatField()
    ctr_v = StatField()

    class Meta:
        model = AccountCreation
        fields = (
            "id", "name", "start", "end", "account", "status", "is_managed",
            "thumbnail", "is_changed", "weekly_chart",
            # delivered stats
            "clicks", "cost", "impressions", "video_views", "video_view_rate",
            "ad_count", "channel_count", "video_count",
            "interest_count", "topic_count", "keyword_count", "is_disapproved",
            "updated_at", "brand", "agency", "from_aw", "cost_method",
            "average_cpv", "average_cpm", "ctr", "ctr_v")

    def __init__(self, *args, **kwargs):
        super(AccountCreationListSerializer, self).__init__(*args, **kwargs)
        self._filter_fields_()
        self.is_chf = self.context.get(
            "request").query_params.get("is_chf") == "1"
        self.settings = {}
        self.stats = {}
        self.struck = {}
        self.daily_chart = defaultdict(list)
        if args:
            if isinstance(args[0], AccountCreation):
                ids = [args[0].id]
            elif type(args[0]) is list:
                ids = [i.id for i in args[0]]
            else:
                ids = [args[0].id]

            settings = CampaignCreation.objects.filter(
                account_creation_id__in=ids
            ).values('account_creation_id').order_by(
                'account_creation_id').annotate(
                start=Min("start"), end=Max("end"),
                video_thumbnail=ConcatAggregate(
                    "ad_group_creations__ad_creations__video_thumbnail",
                    distinct=True)
            )
            self.settings = {s['account_creation_id']: s for s in settings}

            show_client_cost = not registry.user.aw_settings.get(
                UserSettingsKey.DASHBOARD_AD_WORDS_RATES)

            campaign_filter = {self.ACCOUNT_ID_KEY + "__in": ids}
            account_client_cost = dict()
            if show_client_cost:
                account_client_cost = self._get_client_cost_by_account(
                    campaign_filter)

            data = Campaign.objects.filter(**campaign_filter) \
                .values(self.ACCOUNT_ID_KEY) \
                .order_by(self.ACCOUNT_ID_KEY) \
                .annotate(start=Min("start_date"),
                          end=Max("end_date"),
                          **base_stats_aggregator())
            for account_data in data:
                account_id = account_data[self.ACCOUNT_ID_KEY]
                dict_norm_base_stats(account_data)
                dict_add_calculated_stats(account_data)

                if show_client_cost:
                    account_data["cost"] = account_client_cost[account_id]
                self.stats[account_id] = account_data
            annotates = dict(
                ad_count=Count("account__campaigns__ad_groups__ads",
                               distinct=True),
                channel_count=Count(
                    "account__campaigns__ad_groups__channel_statistics__yt_id",
                    distinct=True),
                video_count=Count(
                    "account__campaigns__ad_groups__managed_video_statistics__yt_id",
                    distinct=True),
                interest_count=Count(
                    "account__campaigns__ad_groups__audiences__audience_id",
                    distinct=True),
                topic_count=Count(
                    "account__campaigns__ad_groups__topics__topic_id",
                    distinct=True),
                keyword_count=Count(
                    "account__campaigns__ad_groups__keywords__keyword",
                    distinct=True),
            )
            self.struck = defaultdict(dict)
            for annotate, aggr in annotates.items():
                struck_data = AccountCreation.objects.filter(id__in=ids).values(
                    "id").order_by("id").annotate(
                    **{annotate: aggr}
                )
                for d in struck_data:
                    self.struck[d['id']][annotate] = d[annotate]

            # data for weekly charts
            account_id_key = "ad_group__campaign__account__account_creations__id"
            group_by = (account_id_key, "date")
            daily_stats = AdGroupStatistic.objects.filter(
                ad_group__campaign__account__account_creations__id__in=ids
            ).values(*group_by).order_by(*group_by).annotate(
                views=Sum("video_views")
            )
            for s in daily_stats:
                self.daily_chart[s[account_id_key]].append(
                    dict(label=s['date'], value=s['views']))
            # thumbnails
            group_key = "ad_group__campaign__account__account_creations__id"
            video_ads_data = VideoCreativeStatistic.objects.filter(
                ad_group__campaign__account__account_creations__id__in=ids
            ).values(group_key, "creative_id").order_by(group_key,
                                                        "creative_id").annotate(
                impressions=Sum("impressions"))
            self.video_ads_data = defaultdict(list)
            for v in video_ads_data:
                self.video_ads_data[v[group_key]].append(
                    (v['impressions'], v['creative_id']))

    def _get_client_cost_by_account(self, campaign_filter):
        account_client_cost = defaultdict(float)
        campaigns_with_cost = Campaign.objects.filter(**campaign_filter) \
            .values(self.ACCOUNT_ID_KEY, "impressions", "video_views") \
            .annotate(aw_cost=F("cost"),
                      **client_cost_campaign_required_annotation)

        keys_to_extract = ("goal_type_id", "total_cost", "ordered_rate",
                           "aw_cost", "dynamic_placement", "placement_type",
                           "tech_fee", "impressions", "video_views")

        for campaign_data in campaigns_with_cost:
            account_id = campaign_data[self.ACCOUNT_ID_KEY]
            kwargs = {key: campaign_data.get(key) for key in keys_to_extract}
            client_cost = get_client_cost(**kwargs)
            account_client_cost[account_id] = account_client_cost[account_id] \
                                              + client_cost
        return dict(account_client_cost)

    def _fields_to_exclude(self):
        user = registry.user
        if user.aw_settings.get(UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN):
            return "average_cpv", "average_cpm"
        return tuple()

    def get_from_aw(self, obj):
        return obj.from_aw if not self.is_chf else None

    def get_status(self, obj):
        if not self.is_chf:
            return obj.status
        if obj.is_ended:
            return obj.STATUS_ENDED
        if obj.is_paused:
            return obj.STATUS_PAUSED
        if obj.sync_at or not obj.is_managed:
            return obj.STATUS_RUNNING

    @staticmethod
    def get_name(obj):
        if not obj.is_managed:
            return obj.account.name
        return obj.name

    @staticmethod
    def get_is_disapproved(obj):
        return Ad.objects \
            .filter(is_disapproved=True,
                    ad_group__campaign__account=obj.account) \
            .exists()

    def get_weekly_chart(self, obj):
        return self.daily_chart[obj.id][-7:]

    def get_thumbnail(self, obj):
        video_ads_data = self.video_ads_data.get(obj.id)
        if video_ads_data:
            _, yt_id = sorted(video_ads_data)[-1]
            return "https://i.ytimg.com/vi/{}/hqdefault.jpg".format(yt_id)
        else:
            settings = self.settings.get(obj.id)
            if settings:
                thumbnails = settings['video_thumbnail']
                if thumbnails:
                    return thumbnails.split(", ")[0]

    def get_start(self, obj):
        settings = self.settings.get(obj.id)
        if settings:
            return settings['start']
        else:
            return self.stats.get(obj.id, {}).get("start")

    def get_end(self, obj):
        settings = self.settings.get(obj.id)
        if settings:
            return settings['end']
        else:
            return self.stats.get(obj.id, {}).get("end")

    def get_updated_at(self, obj: AccountCreation):
        if obj.account is not None:
            return safe_max(
                (obj.account.update_time, obj.account.hourly_updated_at))

    def get_brand(self, obj: AccountCreation):
        opportunity = self._get_opportunity(obj)
        return opportunity.brand \
            if opportunity is not None and self.is_chf else None

    def get_agency(self, obj):
        opportunity = self._get_opportunity(obj)
        if opportunity is None or opportunity.agency is None:
            return None
        return opportunity.agency.name if self.is_chf else None

    def get_cost_method(self, obj):
        if not self.is_chf:
            return None
        opportunity = self._get_opportunity(obj)
        if not opportunity:
            return None
        return list(opportunity.goal_types)

    def _get_opportunity(self, obj):
        opportunities = Opportunity.objects.filter(
            placements__adwords_campaigns__account__account_creations=obj)
        if opportunities.count() > 1:
            logger.warning(
                "AccountCreation (id: ) has more then one opportunity".format(
                    obj.id))
        return opportunities.first()
