import logging
from collections import defaultdict
from functools import reduce

from django.db.models import Case
from django.db.models import F
from django.db.models import FloatField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Q
from django.db.models import Sum
from django.db.models import Value
from django.db.models import When
from rest_framework.serializers import BooleanField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField

from aw_creation.api.serializers.common.stats_aggregator import stats_aggregator
from aw_creation.api.serializers.common.utils import get_currency_code
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
from aw_reporting.models import client_cost_campaign_required_annotation
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models.salesforce_constants import ALL_DYNAMIC_PLACEMENTS
from userprofile.constants import UserSettingsKey
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
    plan_cpm=Case(
        When(
            cpm_ordered_units__gt=0,
            then=F("cpm_total_cost") / F("cpm_ordered_units") * 1000
        ),
        default=Value(None),
        output_field=FloatField()
    ),
    plan_cpv=Case(
        When(
            cpv_ordered_units__gt=0,
            then=F("cpv_total_cost") / F("cpv_ordered_units")
        ),
        default=Value(None),
        output_field=FloatField()
    )
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
    cost_method = SerializerMethodField()
    updated_at = SerializerMethodField()
    # analytic data
    all_conversions = StatField()
    impressions = StatField()
    video_views = StatField()
    cost = StatField()
    clicks = StatField()
    video_view_rate = StatField()
    # opportunity data
    brand = SerializerMethodField()
    sf_account = SerializerMethodField()

    statistic_min_date = StatField()
    statistic_max_date = StatField()

    average_cpv = StatField()
    average_cpm = StatField()

    plan_cpv = ParentDictValueField("plan_rates")
    plan_cpm = ParentDictValueField("plan_rates")

    ctr = StatField()
    ctr_v = StatField()
    currency_code = SerializerMethodField()

    class Meta:
        model = AccountCreation
        fields = (
            "account",
            "all_conversions",
            "average_cpm",
            "average_cpv",
            "brand",
            "clicks",
            "cost",
            "cost_method",
            "ctr",
            "ctr_v",
            "currency_code",
            "end",
            "id",
            "impressions",
            "is_changed",
            "is_disapproved",
            "name",
            "plan_cpm",
            "plan_cpv",
            "sf_account",
            "start",
            "statistic_max_date",
            "statistic_min_date",
            "status",
            "thumbnail",
            "updated_at",
            "video_view_rate",
            "video_views",
            "weekly_chart",
        )

    def __init__(self, *args, **kwargs):
        self.user = kwargs["context"]["request"].user
        super(DashboardAccountCreationListSerializer, self).__init__(*args, **kwargs)
        self._filter_fields()
        self.settings = {}
        self.stats = {}
        self.plan_rates = {}
        self.daily_chart = defaultdict(list)
        self.show_client_cost = not self.user.get_aw_settings().get(UserSettingsKey.DASHBOARD_AD_WORDS_RATES)
        if args:
            if isinstance(args[0], AccountCreation):
                ids = [args[0].id]
            elif isinstance(args[0], list):
                ids = [i.id for i in args[0]]
            else:
                ids = [args[0].id]

            self.settings = self._get_settings(ids)
            self.plan_rates = self._get_plan_rates(ids)
            self.stats = self._get_stats(ids)
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
        campaign_filter = {
            self.CAMPAIGN_ACCOUNT_ID_KEY + "__in": account_creation_ids
        }
        flight_filter = {
            self.FLIGHT_ACCOUNT_ID_KEY + "__in": account_creation_ids
        }
        account_client_cost = dict()
        if self.show_client_cost:
            account_client_cost = self._get_client_cost_by_account(
                campaign_filter)

        video_views_impressions = defaultdict(lambda: defaultdict(int))

        queryset = Campaign.objects \
            .filter(**campaign_filter)

        with_ag_type = queryset.annotate(ag_type=Max("ad_groups__type"))
        for campaign in with_ag_type:
            creation_id = campaign.account.account_creation.id
            if campaign.ag_type == "In-stream":
                video_views_impressions[creation_id]["impressions"] += campaign.impressions
                video_views_impressions[creation_id]["views"] += campaign.video_views

        queryset = queryset \
            .values(self.CAMPAIGN_ACCOUNT_ID_KEY) \
            .order_by(self.CAMPAIGN_ACCOUNT_ID_KEY)

        data = queryset \
            .annotate(start=Min("start_date"),
                      end=Max("end_date"),
                      **stats_aggregator())

        dates = queryset.annotate(
            statistic_min_date=Min("statistics__date"),
            statistic_max_date=Max("statistics__date"),
        )
        dates_by_id = {
            item[self.CAMPAIGN_ACCOUNT_ID_KEY]: pick_dict(item, ["statistic_min_date", "statistic_max_date"])
            for item in dates
        }
        flights = Flight.objects.filter(**flight_filter) \
            .distinct() \
            .annotate(account_creation_id=F("placement__adwords_campaigns__account__account_creation__id"),
                      goal_type_id=F("placement__goal_type_id"))

        def accumulate(res, item):
            acc_data = res[item.account_creation_id]
            if item.goal_type_id == SalesForceGoalType.CPV:
                acc_data["cpv_total_cost"] += item.total_cost or 0
                acc_data["cpv_ordered_units"] += item.ordered_units or 0
            elif item.goal_type_id == SalesForceGoalType.CPM:
                acc_data["cpm_total_cost"] += item.total_cost or 0
                acc_data["cpm_ordered_units"] += item.ordered_units or 0
            res[item.account_creation_id] = acc_data
            return res

        sf_data_by_acc = reduce(accumulate, flights, defaultdict(lambda: defaultdict(lambda: 0)))
        for account_data in data:
            account_id = account_data[self.CAMPAIGN_ACCOUNT_ID_KEY]
            account_data.update(dates_by_id[account_id])
            dict_norm_base_stats(account_data)
            account_data["video_views"] = video_views_impressions.get(account_id, {}).get("video_views",
                                                                                          account_data["video_views"])
            account_data["video_impressions"] = video_views_impressions.get(account_id, {}) \
                .get("video_impressions", account_data["video_impressions"])
            dict_add_calculated_stats(account_data)

            if self.show_client_cost:
                cost = account_client_cost[account_id]
                sf_data_for_acc = sf_data_by_acc[account_id]
                cpv_total_costs = sf_data_for_acc["cpv_total_cost"]
                cpm_total_costs = sf_data_for_acc["cpm_total_cost"]
                cpv_ordered_units = sf_data_for_acc["cpv_ordered_units"]
                cpm_ordered_units = sf_data_for_acc["cpm_ordered_units"]
                average_cpv = cpv_total_costs / cpv_ordered_units \
                    if cpv_ordered_units > 0 else None
                average_cpm = cpm_total_costs * 1000 / cpm_ordered_units \
                    if cpm_ordered_units > 0 else None
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
        settings = CampaignCreation.objects \
            .filter(account_creation_id__in=account_creation_ids) \
            .values("account_creation_id") \
            .order_by("account_creation_id") \
            .annotate(start=Min("start"), end=Max("end"),
                      video_thumbnail=ConcatAggregate("ad_group_creations__ad_creations__video_thumbnail",
                                                      distinct=True)
                      )
        return {s["account_creation_id"]: s for s in settings}

    def _get_daily_chart(self, account_creation_ids):
        ids = account_creation_ids
        daily_chart = defaultdict(list)
        account_id_key = "ad_group__campaign__account__account_creation__id"
        group_by = (account_id_key, "date")
        daily_stats = AdGroupStatistic.objects \
            .filter(ad_group__campaign__account__account_creation__id__in=ids) \
            .values(*group_by) \
            .order_by(*group_by) \
            .annotate(views=Sum("video_views"))
        for s in daily_stats:
            daily_chart[s[account_id_key]].append(
                dict(label=s["date"], value=s["views"]))
        return daily_chart

    def _get_video_ads_data(self, account_creation_ids):
        ids = account_creation_ids
        group_key = "ad_group__campaign__account__account_creation__id"
        video_creative_stats = VideoCreativeStatistic.objects \
            .filter(ad_group__campaign__account__account_creation__id__in=ids) \
            .values(group_key, "creative_id") \
            .order_by(group_key, "creative_id") \
            .annotate(impressions=Sum("impressions"))
        video_ads_data = defaultdict(list)
        for v in video_creative_stats:
            video_ads_data[v[group_key]].append(
                (v["impressions"], v["creative_id"]))
        return video_ads_data

    def _fields_to_exclude(self):
        user = self.user
        if user.get_aw_settings().get(UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN):
            return "average_cpv", "average_cpm", "plan_cpm", "plan_cpv", "cost"
        return tuple()

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
        settings = self.settings.get(obj.id)
        if settings:
            thumbnails = settings["video_thumbnail"]
            if thumbnails:
                return thumbnails.split(", ")[0]
        return None

    def get_start(self, obj):
        settings = self.settings.get(obj.id)
        if settings:
            return settings["start"]
        return self.stats.get(obj.id, {}).get("start")

    def get_end(self, obj):
        settings = self.settings.get(obj.id)
        if settings:
            return settings["end"]
        return self.stats.get(obj.id, {}).get("end")

    def get_updated_at(self, obj: AccountCreation):
        return obj.account.update_time if obj.account else None

    def get_brand(self, obj: AccountCreation):
        opportunity = self._get_opportunity(obj)
        return opportunity.brand if opportunity is not None else None

    def get_sf_account(self, obj):
        opportunity = self._get_opportunity(obj)
        if opportunity is None or opportunity.account is None:
            return None
        return opportunity.account.name

    def get_cost_method(self, obj):
        opportunity = self._get_opportunity(obj)
        if not opportunity:
            return None
        return list(opportunity.goal_types)

    def _get_opportunity(self, obj):
        opportunities = Opportunity.objects.filter(placements__adwords_campaigns__account__account_creation=obj) \
            .distinct()
        opp_count = opportunities.count()
        if opp_count > 1:
            logger.warning("AccountCreation (id: %s) has more then one opportunity (%s)", obj.id, opp_count)
        return opportunities.first()

    def get_status(self, obj):
        exists = Campaign.objects.filter(account=obj.account, status="serving").exists()
        if exists:
            status = "Running"
        else:
            status = "Not Running"
        return status

    def get_currency_code(self, obj):
        currency_code = get_currency_code(obj, self.show_client_cost)
        return currency_code
