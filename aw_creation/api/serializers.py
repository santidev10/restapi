from django.db.models import Min, Max, Sum, Q
from rest_framework.serializers import ModelSerializer, \
    SerializerMethodField, ListField, ValidationError, BooleanField
from aw_creation.models import TargetingItem, AdGroupCreation, \
    CampaignCreation, AccountCreation, LocationRule, AdScheduleRule, \
    FrequencyCap, AdGroupOptimizationTuning, CampaignOptimizationTuning, AdCreation
from aw_reporting.models import GeoTarget, Topic, Audience, AdGroupStatistic, \
    Campaign, base_stats_aggregate, dict_norm_base_stats, dict_calculate_stats, ConcatAggregate
from singledb.connector import SingleDatabaseApiConnector, \
    SingleDatabaseApiConnectorException
from decimal import Decimal
from collections import OrderedDict, defaultdict
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)


class SimpleGeoTargetSerializer(ModelSerializer):
    name = SerializerMethodField()

    @staticmethod
    def get_name(obj):
        return obj.canonical_name

    class Meta:
        model = GeoTarget
        fields = ("id", "name", "target_type")


def add_targeting_list_items_info(data, list_type):
    ids = [i['criteria'] for i in data]
    if ids:
        if list_type == TargetingItem.CHANNEL_TYPE:
            connector = SingleDatabaseApiConnector()
            try:
                items = connector.get_custom_query_result(
                    model_name="channel",
                    fields=["id", "title", "thumbnail_image_url"],
                    id__in=ids,
                    limit=len(ids),
                )
                info = {i['id']: i for i in items}
            except SingleDatabaseApiConnectorException as e:
                logger.error(e)
                info = {}

            for item in data:
                item_info = info.get(item['criteria'], {})
                item['id'] = item_info.get("id")
                item['name'] = item_info.get("title")
                item['thumbnail'] = item_info.get("thumbnail_image_url")

        elif list_type == TargetingItem.VIDEO_TYPE:
            connector = SingleDatabaseApiConnector()
            try:
                items = connector.get_custom_query_result(
                    model_name="video",
                    fields=["id", "title", "thumbnail_image_url"],
                    id__in=ids,
                    limit=len(ids),
                )
                info = {i['id']: i for i in items}
            except SingleDatabaseApiConnectorException as e:
                logger.error(e)
                info = {}

            for item in data:
                item_info = info.get(item['criteria'], {})
                item['id'] = item_info.get("id")
                item['name'] = item_info.get("title")
                item['thumbnail'] = item_info.get("thumbnail_image_url")

        elif list_type == TargetingItem.TOPIC_TYPE:
            info = dict(
                Topic.objects.filter(
                    id__in=ids).values_list('id', 'name')
            )
            for item in data:
                item['name'] = info.get(int(item['criteria']))

        elif list_type == TargetingItem.INTEREST_TYPE:
            info = dict(
                Audience.objects.filter(
                    id__in=ids).values_list('id', 'name')
            )
            for item in data:
                item['name'] = info.get(int(item['criteria']))
        elif list_type == TargetingItem.KEYWORD_TYPE:
            for item in data:
                item['name'] = item['criteria']


class CommonTargetingItemSerializerMix:

    @staticmethod
    def get_age_ranges(obj):
        age_ranges = [
            dict(id=uid, name=n)
            for uid, n in AdGroupCreation.AGE_RANGES
            if uid in obj.age_ranges
        ]
        return age_ranges

    @staticmethod
    def get_genders(obj):
        genders = [
            dict(id=uid, name=n)
            for uid, n in AdGroupCreation.GENDERS
            if uid in obj.genders
        ]
        return genders

    @staticmethod
    def get_parents(obj):
        parents = [
            dict(id=uid, name=n)
            for uid, n in AdGroupCreation.PARENTS
            if uid in obj.parents
        ]
        return parents


class AdCreationSetupSerializer(ModelSerializer):
    thumbnail = SerializerMethodField()

    @staticmethod
    def get_thumbnail(obj):
        match = re.match(
            r'(?:https?:/{2})?(?:w{3}\.)?youtu(?:be)?\.(?:com|be)'
            r'(?:/watch\?v=|/video/|/)([^\s&\?]+)',
            obj.video_url,
        )
        if match:
            uid = match.group(1)
            return "https://i.ytimg.com/vi/{}/hqdefault.jpg".format(uid)
        return

    class Meta:
        model = AdCreation
        fields = (
            'id', 'name', 'updated_at',
            'final_url', 'video_url', 'display_url',
            'tracking_template', 'custom_params',
            'thumbnail',
        )


class AdGroupCreationSetupSerializer(CommonTargetingItemSerializerMix, ModelSerializer):

    ad_creations = AdCreationSetupSerializer(many=True, read_only=True)

    targeting = SerializerMethodField()
    age_ranges = SerializerMethodField()
    genders = SerializerMethodField()
    parents = SerializerMethodField()

    @staticmethod
    def get_targeting(obj):
        targeting = {k[0]: [] for k in TargetingItem.TYPES}
        items = obj.targeting_items.all().values(
            'type', 'criteria', 'is_negative')
        for i in items:
            targeting[i['type']].append(i)

        for list_type, items in targeting.items():
            if len(items):
                add_targeting_list_items_info(items, list_type)

        return targeting

    class Meta:
        model = AdGroupCreation
        fields = (
            'id', 'name', 'updated_at',
            'age_ranges', 'genders', 'parents', 'targeting',
            'ad_creations',
        )


class LocationRuleSerializer(ModelSerializer):

    geo_target = SimpleGeoTargetSerializer(read_only=True)
    radius_units = SerializerMethodField()

    @staticmethod
    def get_radius_units(obj):
        units = dict(
            id=obj.radius_units,
            name=dict(LocationRule.UNITS)[obj.radius_units]
        )
        return units

    class Meta:
        model = LocationRule
        exclude = ("id", "campaign_creation")


class AdScheduleSerializer(ModelSerializer):

    class Meta:
        model = AdScheduleRule
        exclude = ("id",)


class FrequencyCapUpdateSerializer(ModelSerializer):

    class Meta:
        model = FrequencyCap


class FrequencyCapSerializer(ModelSerializer):

    event_type = SerializerMethodField()
    level = SerializerMethodField()
    time_unit = SerializerMethodField()

    @staticmethod
    def get_event_type(obj):
        res = dict(
            id=obj.event_type,
            name=dict(FrequencyCap.EVENT_TYPES)[obj.event_type],
        )
        return res

    @staticmethod
    def get_level(obj):
        res = dict(
            id=obj.level,
            name=dict(FrequencyCap.LEVELS)[obj.level],
        )
        return res

    @staticmethod
    def get_time_unit(obj):
        res = dict(
            id=obj.time_unit,
            name=dict(FrequencyCap.TIME_UNITS)[obj.time_unit],
        )
        return res

    class Meta:
        model = FrequencyCap
        exclude = ("id", 'campaign_creation')


class CampaignCreationSetupSerializer(ModelSerializer, CommonTargetingItemSerializerMix):
    ad_group_creations = AdGroupCreationSetupSerializer(many=True, read_only=True)
    location_rules = LocationRuleSerializer(many=True, read_only=True)
    ad_schedule_rules = AdScheduleSerializer(many=True, read_only=True)
    frequency_capping = FrequencyCapSerializer(many=True, read_only=True)

    languages = SerializerMethodField()
    devices = SerializerMethodField()
    video_ad_format = SerializerMethodField()
    delivery_method = SerializerMethodField()
    video_networks = SerializerMethodField()

    age_ranges = SerializerMethodField()
    genders = SerializerMethodField()
    parents = SerializerMethodField()
    content_exclusions = SerializerMethodField()

    @staticmethod
    def get_content_exclusions(obj):
        content_exclusions = [
            dict(id=uid, name=n)
            for uid, n in CampaignCreation.CONTENT_LABELS
            if uid in obj.content_exclusions
        ]
        return content_exclusions

    @staticmethod
    def get_video_ad_format(obj):
        item_id = obj.video_ad_format
        options = dict(obj.__class__.VIDEO_AD_FORMATS)
        return dict(id=item_id, name=options[item_id])

    @staticmethod
    def get_delivery_method(obj):
        item_id = obj.delivery_method
        options = dict(obj.__class__.DELIVERY_METHODS)
        return dict(id=item_id, name=options[item_id])

    @staticmethod
    def get_video_networks(obj):
        ids = obj.video_networks
        video_networks = [
            dict(id=uid, name=n)
            for uid, n in obj.__class__.VIDEO_NETWORKS
            if uid in ids
        ]
        return video_networks

    @staticmethod
    def get_languages(obj):
        languages = obj.languages.values('id', 'name')
        return languages

    @staticmethod
    def get_devices(obj):
        ids = obj.devices
        devices = [
            dict(id=uid, name=n)
            for uid, n in CampaignCreation.DEVICES
            if uid in ids
        ]
        return devices

    class Meta:
        model = CampaignCreation
        fields = (
            'id', 'name', 'updated_at',
            'start', 'end', 'budget', 'languages',
            'devices', 'location_rules', 'frequency_capping', 'ad_schedule_rules',
            'video_networks', 'delivery_method', 'video_ad_format',
            'age_ranges', 'genders', 'parents',
            'content_exclusions',
            'ad_group_creations',
        )


class StatField(SerializerMethodField):
    def to_representation(self, value):
        return self.parent.stats.get(value.id, {}).get(self.field_name)


class AccountCreationListSerializer(ModelSerializer):
    is_changed = BooleanField()
    is_optimization_active = SerializerMethodField()
    weekly_chart = SerializerMethodField()
    status = SerializerMethodField()
    start = SerializerMethodField()
    end = SerializerMethodField()
    impressions = StatField()
    video_views = StatField()
    cost = StatField()
    clicks = StatField()
    video_view_rate = StatField()
    ctr_v = StatField()

    def get_weekly_chart(self, obj):
        return self.daily_chart[obj.id][-7:]

    @staticmethod
    def get_is_optimization_active(*_):
        return True

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

    @staticmethod
    def get_status(obj):
        if obj.is_ended:
            s = "ended"
        elif obj.is_paused:
            s = "paused"
        elif obj.account:
            s = "running"
        elif obj.is_approved:
            s = "approved"
        else:
            s = "pending"
        return s.capitalize()

    def __init__(self, instance=None, *args, **kwargs):
        self.settings = {}
        self.stats = {}
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
            ).values('account_creation_id').order_by('account_creation_id').annotate(
                start=Min("start"), end=Max("end"),
            )
            self.settings = {s['account_creation_id']: s for s in settings}

            data = Campaign.objects.filter(
                account__account_creation_id__in=ids
            ).values('account__account_creation_id').order_by('account__account_creation_id').annotate(
                start=Min("start_date"),
                end=Max("end_date"),
                **base_stats_aggregate
            )
            for i in data:
                dict_norm_base_stats(i)
                dict_calculate_stats(i)
                self.stats[i['account__account_creation_id']] = i

            # data for weekly charts
            account_id_key = "ad_group__campaign__account__account_creation_id"
            group_by = (account_id_key, "date")
            daily_stats = AdGroupStatistic.objects.filter(
                ad_group__campaign__account_id__in=ids
            ).values(*group_by).order_by(*group_by).annotate(
                views=Sum("video_views")
            )
            for s in daily_stats:
                self.daily_chart[s[account_id_key]].append(
                    dict(label=s['date'], value=s['views'])
                )

        super(AccountCreationListSerializer, self).__init__(instance, *args, **kwargs)

    class Meta:
        model = AccountCreation
        fields = (
            "id", "name", "start", "end", "status",
            "is_optimization_active", "is_changed", "weekly_chart",
            # delivered stats
            'clicks', 'cost', 'impressions', 'video_views', 'video_view_rate', 'ctr_v',
        )


class AccountCreationSetupSerializer(ModelSerializer):

    campaign_creations = CampaignCreationSetupSerializer(many=True,
                                                         read_only=True)

    class Meta:
        model = AccountCreation
        fields = ('id', 'name', 'is_ended', 'is_approved', 'is_paused',
                  'campaign_creations', 'updated_at')


class AccountCreationUpdateSerializer(ModelSerializer):

    class Meta:
        model = AccountCreation
        fields = (
            'name',
            'is_ended',
            'is_paused',
            'is_approved',
        )


class CampaignCreationUpdateSerializer(ModelSerializer):
    video_networks = ListField()
    devices = ListField()
    genders = ListField()
    parents = ListField()
    age_ranges = ListField()
    content_exclusions = ListField()

    class Meta:
        model = CampaignCreation
        fields = (
            'name', 'start', 'end', 'budget',
            'languages', 'devices',
            'video_ad_format', 'delivery_method', 'video_networks',
            'genders', 'parents', 'age_ranges',
            'content_exclusions',
        )

    def validate(self, data):
        if "devices" in data and not data["devices"]:
            raise ValidationError("devices: empty set is not allowed")

        for f in ('devices', 'video_networks', 'languages', 'genders', 'parents', 'age_ranges'):
            if f in data and not data[f]:
                raise ValidationError(
                    "{}: empty set is not allowed".format(f))

        if 'video_networks' in data:
            video_networks = data['video_networks']
            if CampaignCreation.VIDEO_PARTNER_DISPLAY_NETWORK in video_networks and\
               CampaignCreation.YOUTUBE_VIDEO not in video_networks:
                raise ValidationError(
                    "Cannot target display network without first "
                    "targeting YouTube video network")

        # if one of the following fields is provided
        if {"start", "end"} & set(data.keys()):
            today = datetime.now().date()

            start, end = None, None
            if self.instance:
                start, end = self.instance.start, self.instance.end

            if data.get("start"):
                start = data.get("start")
            if data.get("end"):
                end = data.get("end")

            for f_name, date in (("start", start), ("end", end)):
                if date and date < today:
                    if data.get(f_name) or data.get("is_approved") is True:
                        raise ValidationError(
                            'Wrong date period: '
                            'dates in the past are not allowed')

            if start and end and start > end:
                raise ValidationError(
                    'Wrong date period: start date > end date')

        return super(CampaignCreationUpdateSerializer, self).validate(data)


class AppendCampaignCreationSerializer(ModelSerializer):

    class Meta:
        model = CampaignCreation
        fields = (
            'name', 'account_creation',
        )


class OptimizationLocationRuleUpdateSerializer(ModelSerializer):
    class Meta:
        model = LocationRule


class AdGroupCreationUpdateSerializer(ModelSerializer):
    genders = ListField()
    parents = ListField()
    age_ranges = ListField()

    class Meta:
        model = AdGroupCreation
        exclude = ('genders_raw', 'age_ranges_raw', 'parents_raw')


class AppendAdGroupCreationSetupSerializer(ModelSerializer):

    class Meta:
        model = AdGroupCreation
        fields = (
            'name', 'campaign_creation',
        )


class AdCreationUpdateSerializer(ModelSerializer):
    custom_params = ListField()

    class Meta:
        model = AdCreation
        exclude = ('ad_group_creation',)

    def validate(self, data):

        # approving process
        custom_params = data.get("custom_params")
        if custom_params:
            if len(custom_params) > 3:
                raise ValidationError(
                    'You cannot use more than 3 custom parameters'
                )
            for i in custom_params:
                if type(i) is not dict or set(i.keys()) != {"name", "value"}:
                    raise ValidationError(
                        'Custom parameters format is [{"name": "ad", "value": "demo"}, ..]'
                    )

        return super(AdCreationUpdateSerializer, self).validate(data)


class AppendAdCreationSetupSerializer(ModelSerializer):

    class Meta:
        model = AdCreation
        fields = ('name', 'ad_group_creation')


class TopicHierarchySerializer(ModelSerializer):
    children = SerializerMethodField()

    class Meta:
        model = Topic
        exclude = ("parent",)

    @staticmethod
    def get_children(obj):
        r = TopicHierarchySerializer(obj.children.all(), many=True).data
        return r


class AudienceHierarchySerializer(ModelSerializer):
    children = SerializerMethodField()

    class Meta:
        model = Audience
        exclude = ("parent",)

    @staticmethod
    def get_children(obj):
        r = AudienceHierarchySerializer(obj.children.all(),
                                        many=True).data
        return r


class AdGroupTargetingListSerializer(ModelSerializer):

    class Meta:
        model = TargetingItem
        exclude = ('type', 'id', 'ad_group_creation')


class AdGroupTargetingListUpdateSerializer(ModelSerializer):

    class Meta:
        model = TargetingItem
        exclude = ('id',)


# Optimize / Optimization

class OptimizationFiltersAdGroupSerializer(ModelSerializer):

    class Meta:
        model = AdGroupCreation
        fields = ('id', 'name')


class OptimizationFiltersCampaignSerializer(ModelSerializer):

    ad_group_creations = SerializerMethodField()

    def __init__(self, *args, kpi, **kwargs):
        self.kpi = kpi
        super(OptimizationFiltersCampaignSerializer, self).__init__(
                *args, **kwargs)

    def get_ad_group_creations(self, obj):
        queryset = AdGroupCreation.objects.filter(
            campaign_creation=obj,
        ).filter(
            Q(optimization_tuning__value__isnull=False,
              optimization_tuning__kpi=self.kpi) |
            Q(campaign_creation__optimization_tuning__value__isnull=False,
              campaign_creation__optimization_tuning__kpi=self.kpi)
        ).distinct()
        items = OptimizationFiltersAdGroupSerializer(queryset, many=True).data
        return items

    class Meta:
        model = CampaignCreation
        fields = ('id', 'name', 'ad_group_creations')


class OptimizationSettingsAdGroupSerializer(ModelSerializer):

    value = SerializerMethodField()

    def get_value(self, obj):
        kpi = self.parent.parent.parent.parent.kpi
        try:
            s = AdGroupOptimizationTuning.objects.get(item=obj, kpi=kpi)
        except AdGroupOptimizationTuning.DoesNotExist:
            pass
        else:
            return s.value

    class Meta:
        model = AdGroupCreation
        fields = ('id', 'name', 'value')


class OptimizationSettingsCampaignsSerializer(ModelSerializer):

    ad_group_creations = OptimizationSettingsAdGroupSerializer(many=True)
    value = SerializerMethodField()

    def get_value(self, obj):
        kpi = self.parent.parent.kpi
        try:
            s = CampaignOptimizationTuning.objects.get(item=obj, kpi=kpi)
        except CampaignOptimizationTuning.DoesNotExist:
            pass
        else:
            return s.value

    class Meta:
        model = CampaignCreation
        fields = ('id', 'name', 'value', 'ad_group_creations')


class OptimizationSettingsSerializer(ModelSerializer):

    def __init__(self, *args, kpi, **kwargs):
        self.kpi = kpi
        super(OptimizationSettingsSerializer, self).__init__(*args, **kwargs)

    campaign_creations = OptimizationSettingsCampaignsSerializer(many=True)

    class Meta:
        model = AccountCreation
        fields = ('id', 'name', 'campaign_creations')
