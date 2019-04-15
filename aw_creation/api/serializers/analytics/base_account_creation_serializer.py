import logging
from collections import defaultdict

from django.db.models import Avg
from django.db.models import Case
from django.db.models import CharField
from django.db.models import Count
from django.db.models import F
from django.db.models import FloatField
from django.db.models import FloatField as AggrFloatField
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
from aw_reporting.models import Ad
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AgeRanges
from aw_reporting.models import CONVERSIONS
from aw_reporting.models import Campaign
from aw_reporting.models import GenderStatistic
from aw_reporting.models import Genders
from aw_reporting.models import OpPlacement
from aw_reporting.models import QUARTILE_STATS
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import device_str
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models import dict_quartiles_to_rates
from aw_reporting.models.salesforce_constants import ALL_DYNAMIC_PLACEMENTS
from singledb.connector import SingleDatabaseApiConnector
from singledb.connector import SingleDatabaseApiConnectorException
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
    plan_cpm=Case(When(
        ~Q(cpm_ordered_units=0),
        then=F("cpm_total_cost") / F("cpm_ordered_units") * 1000
    )),
    plan_cpv=Case(When(
        ~Q(cpv_ordered_units=0),
        then=F("cpv_total_cost") / F("cpv_ordered_units")
    ))
)
CAMPAIGN_ACCOUNT_ID_KEY = "account__account_creation__id"


class BaseAccountCreationSerializer(ModelSerializer, ExcludeFieldsMixin):
    is_changed = BooleanField()
    name = SerializerMethodField()
    thumbnail = SerializerMethodField()
    weekly_chart = SerializerMethodField()
    start = SerializerMethodField()
    end = SerializerMethodField()
    is_disapproved = SerializerMethodField()
    from_aw = BooleanField()
    status = CharField()
    updated_at = SerializerMethodField()
    is_managed = BooleanField()
    is_editable = SerializerMethodField()
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

    average_cpv = StatField()
    average_cpm = StatField()

    statistic_min_date = StatField()
    statistic_max_date = StatField()

    plan_cpv = ParentDictValueField("plan_rates")
    plan_cpm = ParentDictValueField("plan_rates")

    ctr = StatField()
    ctr_v = StatField()

    details = SerializerMethodField()

    stats_aggregations = dict()

    class Meta:
        model = AccountCreation

    def __init__(self, *args, **kwargs):
        super(BaseAccountCreationSerializer, self).__init__(*args, **kwargs)
        self._filter_fields()
        self.settings = {}
        self.stats = {}
        self.plan_rates = {}
        self.struck = {}
        self.daily_chart = defaultdict(list)
        self.user = kwargs.get("context").get("request").user
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

    def _get_stats(self, account_creation_ids):
        stats = {}
        campaign_filter = {
            CAMPAIGN_ACCOUNT_ID_KEY + "__in": account_creation_ids
        }

        data = Campaign.objects \
            .filter(**campaign_filter) \
            .values(CAMPAIGN_ACCOUNT_ID_KEY) \
            .order_by(CAMPAIGN_ACCOUNT_ID_KEY) \
            .annotate(start=Min("start_date"),
                      end=Max("end_date"),
                      statistic_min_date=Min("statistics__date"),
                      statistic_max_date=Max("statistics__date"),
                      **self.stats_aggregations)
        for account_data in data:
            account_id = account_data[CAMPAIGN_ACCOUNT_ID_KEY]
            dict_norm_base_stats(account_data)
            dict_add_calculated_stats(account_data)

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
        return obj.account.update_time if obj.account else None

    def get_is_editable(self, obj):
        return obj.owner == self.user

    def get_details(self, account_creation):
        ads_and_placements_stats = {s: Sum(s) for s in
                                    CONVERSIONS + QUARTILE_STATS}

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
        device = [dict(name=device_str(i['device_id']), value=i['v']) for i in
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
