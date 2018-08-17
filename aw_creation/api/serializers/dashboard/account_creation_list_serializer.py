import logging
from collections import defaultdict

from django.db.models import Case
from django.db.models import Count
from django.db.models import F
from django.db.models import FloatField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Q
from django.db.models import Sum
from django.db.models import When
from rest_framework.serializers import BooleanField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField

from aw_creation.api.serializers.common.struck_field import StruckField
from aw_creation.models import AccountCreation
from aw_creation.models import CampaignCreation
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.models import Ad
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import base_stats_aggregator
from aw_reporting.models import client_cost_campaign_required_annotation
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models.salesforce_constants import ALL_DYNAMIC_PLACEMENTS
from aw_reporting.utils import safe_max
from userprofile.models import UserSettingsKey
from utils.db.aggregators import ConcatAggregate
from utils.lang import pick_dict
from utils.serializers import ExcludeFieldsMixin
from utils.serializers.fields import ParentDictValueField
from utils.serializers.fields import StatField

logger = logging.getLogger(__name__)

dynamic_placement_q = Q(dynamic_placement__in=ALL_DYNAMIC_PLACEMENTS)
outgoing_fee_q = ~dynamic_placement_q \
                 & Q(placement_type=OpPlacement.OUTGOING_FEE_TYPE)
hard_cost_q = ~dynamic_placement_q & ~outgoing_fee_q \
              & Q(goal_type_id=SalesForceGoalType.HARD_COST)
regular_placement_q = ~dynamic_placement_q & ~outgoing_fee_q \
                      & Q(goal_type_id__in=[SalesForceGoalType.CPV,
                                            SalesForceGoalType.CPM])
regular_cpv_q = regular_placement_q & Q(goal_type_id=SalesForceGoalType.CPV)
regular_cpm_q = regular_placement_q & Q(goal_type_id=SalesForceGoalType.CPM)

PLAN_STATS_ANNOTATION = dict(
    cpv_ordered_units=Sum(Case(When(regular_cpv_q, then=F("ordered_units")),
                               output_field=FloatField())),
    cpm_ordered_units=Sum(Case(When(regular_cpm_q, then=F("ordered_units")),
                               output_field=FloatField())),
    cpv_total_cost=Sum(Case(When(regular_cpv_q, then=F("total_cost")))),
    cpm_total_cost=Sum(Case(When(regular_cpm_q, then=F("total_cost")))),
)

PLAN_RATES_ANNOTATION = dict(
    plan_cpm=F("cpm_total_cost") / F("cpm_ordered_units") * 1000,
    plan_cpv=F("cpv_total_cost") / F("cpv_ordered_units")
)

FLIGHTS_AGGREGATIONS = dict(
    cpv_total_costs=Sum(Case(
        When(placement__goal_type_id=SalesForceGoalType.CPV,
             then="total_cost"))),
    cpm_total_costs=Sum(Case(
        When(placement__goal_type_id=SalesForceGoalType.CPM,
             then="total_cost"))),
    cpv_ordered_units=Sum(Case(
        When(placement__goal_type_id=SalesForceGoalType.CPV,
             then="ordered_units"))),
    cpm_ordered_units=Sum(Case(
        When(placement__goal_type_id=SalesForceGoalType.CPM,
             then="ordered_units")))
)


class DashboardAccountCreationListSerializer(ModelSerializer, ExcludeFieldsMixin):
    CAMPAIGN_ACCOUNT_ID_KEY = "account__account_creation__id"
    FLIGHT_ACCOUNT_ID_KEY = "placement__adwords_campaigns__" + CAMPAIGN_ACCOUNT_ID_KEY
    is_changed = BooleanField()
    name = SerializerMethodField()
    thumbnail = SerializerMethodField()
    weekly_chart = SerializerMethodField()
    start = SerializerMethodField()
    end = SerializerMethodField()
    is_disapproved = SerializerMethodField()
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

    plan_cpv = ParentDictValueField("plan_rates")
    plan_cpm = ParentDictValueField("plan_rates")

    ctr = StatField()
    ctr_v = StatField()

    class Meta:
        model = AccountCreation
        fields = (
            "id", "name", "start", "end", "account", "status",
            "thumbnail", "is_changed", "weekly_chart",
            # delivered stats
            "clicks", "cost", "impressions", "video_views", "video_view_rate",
            "ad_count", "channel_count", "video_count", "interest_count",
            "topic_count", "keyword_count", "is_disapproved", "updated_at",
            "brand", "agency", "cost_method", "average_cpv",
            "average_cpm", "ctr", "ctr_v", "plan_cpm",
            "plan_cpv"
        )

    def __init__(self, *args, **kwargs):
        self.user = kwargs["context"]["request"].user
        super(DashboardAccountCreationListSerializer, self).__init__(*args, **kwargs)
        self._filter_fields()
        self.settings = {}
        self.stats = {}
        self.plan_rates = {}
        self.struck = {}
        self.daily_chart = defaultdict(list)
        if args:
            if isinstance(args[0], AccountCreation):
                ids = [args[0].id]
            elif type(args[0]) is list:
                ids = [i.id for i in args[0]]
            else:
                ids = [args[0].id]

            self.settings = self._get_settings(ids)
            self.plan_rates = self._get_plan_rates(ids)
            self.stats = self._get_stats(ids)
            self.struck = self._get_struck(ids)
            self.daily_chart = self._get_daily_chart(ids)
            self.video_ads_data = self._get_video_ads_data(ids)

    def _get_client_cost_by_account(self, campaign_filter):
        account_client_cost = defaultdict(float)
        campaigns_with_cost = Campaign.objects.filter(**campaign_filter) \
            .values(self.CAMPAIGN_ACCOUNT_ID_KEY, "impressions", "video_views") \
            .annotate(aw_cost=F("cost"),
                      start=F("start_date"), end=F("end_date"),
                      **client_cost_campaign_required_annotation)

        keys_to_extract = ("goal_type_id", "total_cost", "ordered_rate",
                           "aw_cost", "dynamic_placement", "placement_type",
                           "tech_fee", "impressions", "video_views",
                           "start", "end")

        for campaign_data in campaigns_with_cost:
            account_id = campaign_data[self.CAMPAIGN_ACCOUNT_ID_KEY]
            kwargs = {key: campaign_data.get(key) for key in keys_to_extract}
            client_cost = get_client_cost(**kwargs)
            account_client_cost[account_id] = account_client_cost[account_id] \
                                              + client_cost
        return dict(account_client_cost)

    def _get_stats(self, account_creation_ids):
        stats = {}
        show_client_cost = not self.user.get_aw_settings().get(UserSettingsKey.DASHBOARD_AD_WORDS_RATES)
        campaign_filter = {
            self.CAMPAIGN_ACCOUNT_ID_KEY + "__in": account_creation_ids
        }
        flight_filter = {
            self.FLIGHT_ACCOUNT_ID_KEY + "__in": account_creation_ids
        }
        account_client_cost = dict()
        if show_client_cost:
            account_client_cost = self._get_client_cost_by_account(
                campaign_filter)

        data = Campaign.objects.filter(**campaign_filter) \
            .values(self.CAMPAIGN_ACCOUNT_ID_KEY) \
            .order_by(self.CAMPAIGN_ACCOUNT_ID_KEY) \
            .annotate(start=Min("start_date"),
                      end=Max("end_date"),
                      **base_stats_aggregator())
        sf_data_annotated = Flight.objects.filter(**flight_filter) \
            .values(self.FLIGHT_ACCOUNT_ID_KEY) \
            .order_by(self.FLIGHT_ACCOUNT_ID_KEY) \
            .annotate(**FLIGHTS_AGGREGATIONS)
        sf_data_by_acc = {i[self.FLIGHT_ACCOUNT_ID_KEY]: i
                          for i in sf_data_annotated}
        for account_data in data:
            account_id = account_data[self.CAMPAIGN_ACCOUNT_ID_KEY]
            dict_norm_base_stats(account_data)
            dict_add_calculated_stats(account_data)

            if show_client_cost:
                cost = account_client_cost[account_id]
                sf_data_for_acc = sf_data_by_acc.get(account_id) or dict()
                cpv_total_costs = sf_data_for_acc.get("cpv_total_costs") or 0
                cpm_total_costs = sf_data_for_acc.get("cpm_total_costs") or 0
                cpv_ordered_units = sf_data_for_acc.get(
                    "cpv_ordered_units") or 0
                cpm_ordered_units = sf_data_for_acc.get(
                    "cpm_ordered_units") or 0
                average_cpv = cpv_total_costs / cpv_ordered_units \
                    if cpv_ordered_units else None
                average_cpm = cpm_total_costs * 1000 / cpm_ordered_units \
                    if cpm_ordered_units else None
                account_data["cost"] = cost
                account_data["average_cpm"] = average_cpm
                account_data["average_cpv"] = average_cpv
            stats[account_id] = account_data
        return stats

    def _get_plan_rates(self, account_creation_ids):
        account_creation_ref = "adwords_campaigns__account__account_creation__id"
        keys = list(PLAN_RATES_ANNOTATION.keys())
        stats = OpPlacement.objects \
            .filter(**{account_creation_ref + "__in": account_creation_ids}) \
            .values(account_creation_ref) \
            .order_by(account_creation_ref) \
            .annotate(**PLAN_STATS_ANNOTATION) \
            .annotate(**PLAN_RATES_ANNOTATION) \
            .values(account_creation_ref, *keys)
        return {s[account_creation_ref]: pick_dict(s, keys) for s in stats}

    def _get_settings(self, account_creation_ids):
        settings = CampaignCreation.objects.filter(
            account_creation_id__in=account_creation_ids
        ).values('account_creation_id').order_by(
            'account_creation_id').annotate(
            start=Min("start"), end=Max("end"),
            video_thumbnail=ConcatAggregate(
                "ad_group_creations__ad_creations__video_thumbnail",
                distinct=True)
        )
        return {s['account_creation_id']: s for s in settings}

    def _get_struck(self, account_creation_ids):
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
        struck = defaultdict(dict)
        for annotate, aggr in annotates.items():
            struck_data = AccountCreation.objects \
                .filter(id__in=account_creation_ids) \
                .values("id") \
                .order_by("id") \
                .annotate(**{annotate: aggr})
            for d in struck_data:
                struck[d['id']][annotate] = d[annotate]
        return struck

    def _get_daily_chart(self, account_creation_ids):
        ids = account_creation_ids
        daily_chart = defaultdict(list)
        account_id_key = "ad_group__campaign__account__account_creation__id"
        group_by = (account_id_key, "date")
        daily_stats = AdGroupStatistic.objects.filter(
            ad_group__campaign__account__account_creation__id__in=ids
        ).values(*group_by).order_by(*group_by).annotate(
            views=Sum("video_views")
        )
        for s in daily_stats:
            daily_chart[s[account_id_key]].append(
                dict(label=s['date'], value=s['views']))
        return daily_chart

    def _get_video_ads_data(self, account_creation_ids):
        ids = account_creation_ids
        group_key = "ad_group__campaign__account__account_creation__id"
        video_creative_stats = VideoCreativeStatistic.objects.filter(
            ad_group__campaign__account__account_creation__id__in=ids
        ).values(group_key, "creative_id").order_by(group_key,
                                                    "creative_id").annotate(
            impressions=Sum("impressions"))
        video_ads_data = defaultdict(list)
        for v in video_creative_stats:
            video_ads_data[v[group_key]].append(
                (v['impressions'], v['creative_id']))
        return video_ads_data

    def _fields_to_exclude(self):
        user = self.user
        if user.get_aw_settings().get(UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN):
            return "average_cpv", "average_cpm", "plan_cpm", "plan_cpv", "cost"
        return tuple()

    def get_status(self, obj):
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
        return opportunity.brand if opportunity is not None else None

    def get_agency(self, obj):
        opportunity = self._get_opportunity(obj)
        if opportunity is None or opportunity.agency is None:
            return None
        return opportunity.agency.name

    def get_cost_method(self, obj):
        opportunity = self._get_opportunity(obj)
        if not opportunity:
            return None
        return list(opportunity.goal_types)

    def _get_opportunity(self, obj):
        opportunities = Opportunity.objects.filter(
            placements__adwords_campaigns__account__account_creation=obj)
        if opportunities.count() > 1:
            logger.warning(
                "AccountCreation (id: ) has more then one opportunity".format(
                    obj.id))
        return opportunities.first()
