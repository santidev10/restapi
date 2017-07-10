from django.db.models import QuerySet, Min, Max, Count, F, Case, When, Sum, Q, \
    IntegerField as AggrIntegerField, FloatField as AggrFloatField, \
    DecimalField as AggrDecimalField, DateField as AggrDateField
from rest_framework.serializers import ModelSerializer, \
    SerializerMethodField, ListField, ValidationError
from aw_reporting.utils import get_dates_range
from aw_creation.models import TargetingItem, AdGroupCreation, \
    CampaignCreation, AccountCreation, LocationRule, AdScheduleRule, \
    FrequencyCap, AdGroupOptimizationTuning, CampaignOptimizationTuning, \
    get_yt_id_from_url
from aw_reporting.models import AdGroup, VideoCreativeStatistic, GeoTarget, Topic, Audience, DATE_FORMAT, SUM_STATS, \
    dict_add_calculated_stats, YTChannelStatistic, YTVideoStatistic, KeywordStatistic, AdGroupStatistic
from singledb.connector import SingleDatabaseApiConnector, \
    SingleDatabaseApiConnectorException
from decimal import Decimal
from collections import OrderedDict
from datetime import datetime
import math
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


class OptimizationAdGroupSerializer(ModelSerializer):

    thumbnail = SerializerMethodField()
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
        model = AdGroupCreation
        fields = (
            'id', 'name', 'is_approved', 'max_rate',
            'final_url',
            'video_url',
            'ct_overlay_text',
            'display_url',
            'thumbnail',
            'age_ranges', 'genders', 'parents',
            'targeting',
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


class OptimizationCampaignsSerializer(ModelSerializer):
    ad_group_creations = OptimizationAdGroupSerializer(many=True, read_only=True)
    location_rules = LocationRuleSerializer(many=True, read_only=True)
    ad_schedule_rules = AdScheduleSerializer(many=True, read_only=True)
    frequency_capping = FrequencyCapSerializer(many=True, read_only=True)

    languages = SerializerMethodField()
    devices = SerializerMethodField()
    video_ad_format = SerializerMethodField()
    type = SerializerMethodField()
    goal_type = SerializerMethodField()
    delivery_method = SerializerMethodField()
    bidding_type = SerializerMethodField()
    video_networks = SerializerMethodField()

    @staticmethod
    def get_video_ad_format(obj):
        item_id = obj.video_ad_format
        options = dict(obj.__class__.VIDEO_AD_FORMATS)
        return dict(id=item_id, name=options[item_id])

    @staticmethod
    def get_type(obj):
        item_id = obj.type
        options = dict(obj.__class__.CAMPAIGN_TYPES)
        return dict(id=item_id, name=options[item_id])

    @staticmethod
    def get_goal_type(obj):
        item_id = obj.goal_type
        options = dict(obj.__class__.GOAL_TYPES)
        return dict(id=item_id, name=options[item_id])

    @staticmethod
    def get_delivery_method(obj):
        item_id = obj.delivery_method
        options = dict(obj.__class__.DELIVERY_METHODS)
        return dict(id=item_id, name=options[item_id])

    @staticmethod
    def get_bidding_type(obj):
        item_id = obj.bidding_type
        options = dict(obj.__class__.BIDDING_TYPES)
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
            'id', 'name',
            'is_paused', 'is_approved',
            'start', 'end',
            'budget', 'max_rate', 'languages',
            'ad_group_creations',
            'devices', 'location_rules', 'frequency_capping', 'ad_schedule_rules',
            'goal_units',
            'bidding_type', 'video_networks', 'delivery_method', 'video_ad_format', 'type', 'goal_type',
        )


class AccountCreationListSerializer(ModelSerializer):
    is_optimization_active = SerializerMethodField()
    status = SerializerMethodField()
    goal_units = SerializerMethodField()
    start = SerializerMethodField()
    end = SerializerMethodField()
    weekly_chart = SerializerMethodField()
    campaigns_count = SerializerMethodField()
    ad_groups_count = SerializerMethodField()
    creative_count = SerializerMethodField()
    channels_count = SerializerMethodField()
    videos_count = SerializerMethodField()
    keywords_count = SerializerMethodField()

    structure = SerializerMethodField()
    creative = SerializerMethodField()
    goal_charts = SerializerMethodField()

    @staticmethod
    def get_goal_units(obj):
        data = obj.campaign_creations.aggregate(goal_units=Sum('goal_units'))
        return data['goal_units']

    @staticmethod
    def get_start(obj):
        data = obj.campaign_creations.aggregate(value=Min('start'))
        return data['value']

    @staticmethod
    def get_end(obj):
        data = obj.campaign_creations.aggregate(value=Max('end'))
        return data['value']

    @staticmethod
    def get_goal_charts(obj):
        charts = []
        data = obj.campaign_creations.aggregate(
            start=Min('start'), end=Max('end'), goal_units=Sum('goal_units'),
        )
        start, end, ordered_units = data["start"], data["end"], data["goal_units"]
        if start and end and ordered_units:
            dates = list(get_dates_range(start, end))
            daily = ordered_units / len(dates)
            values = [math.ceil((n + 1) * daily)
                      for n in range(len(dates))]
            goal_chart = dict(
                label='View Goal',
                value=ordered_units,
                trend=[
                    dict(label=d, value=v)
                    for d, v in zip(dates, values)
                ]
            )
            charts.append(goal_chart)

        if obj.account:
            stats = AdGroupStatistic.objects.filter(
                ad_group__campaign__account=obj.account
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

        def get_ad_groups(cid):
            return AdGroupCreation.objects.filter(campaign_creation_id=cid)

        structure = [
            dict(
                id=c['id'],
                name=c['name'],
                ad_group_creations=[
                    dict(id=a['id'], name=a['name'])
                    for a in get_ad_groups(c['id']).values('id', 'name').order_by('name')
                ]
            )
            for c in obj.campaign_creations.values("id", "name").order_by("name")
        ]
        return structure

    @staticmethod
    def get_creative(obj):
        video_urls = AdGroupCreation.objects.filter(
            campaign_creation__account_creation_id=obj
        ).values_list("video_url", flat=True).order_by("video_url").distinct()[:1]
        video_ids = list(filter(None, set(get_yt_id_from_url(url) for url in video_urls)))

        if video_ids:
            video_id = video_ids[0]
            connector = SingleDatabaseApiConnector()
            try:
                items = connector.get_custom_query_result(
                    model_name="video",
                    fields=["id", "title", "thumbnail_image_url"],
                    id=video_id,
                    limit=1,
                )
            except SingleDatabaseApiConnectorException as e:
                logger.critical(e)
            else:
                if items:
                    item = items[0]
                    response = dict(
                        id=item['id'],
                        name=item['title'],
                        thumbnail=item['thumbnail_image_url'],
                    )
                    return response
            response = dict(
                name=video_id,
                thumbnail="https://i.ytimg.com/vi/{}/hqdefault.jpg".format(video_id)
            )
            return response

    @staticmethod
    def get_channels_count(obj):
        c = TargetingItem.objects.filter(
            ad_group_creation__campaign_creation__account_creation=obj,
            type=TargetingItem.CHANNEL_TYPE
        ).count()
        return c

    @staticmethod
    def get_videos_count(obj):
        c = TargetingItem.objects.filter(
            ad_group_creation__campaign_creation__account_creation=obj,
            type=TargetingItem.VIDEO_TYPE
        ).count()
        return c

    @staticmethod
    def get_keywords_count(obj):
        c = TargetingItem.objects.filter(
            ad_group_creation__campaign_creation__account_creation=obj,
            type=TargetingItem.KEYWORD_TYPE
        ).count()
        return c

    @staticmethod
    def get_campaigns_count(obj):
        return obj.campaign_creations.count()

    @staticmethod
    def get_ad_groups_count(obj):
        return AdGroupCreation.objects.filter(campaign_creation__account_creation=obj).count()

    @staticmethod
    def get_creative_count(obj):
        return AdGroupCreation.objects.filter(campaign_creation__account_creation=obj).count()

    @staticmethod
    def get_weekly_chart(obj):
        if obj.account:
            data = AdGroupStatistic.objects.filter(
                ad_group__campaign__account=obj.account
            ).values("date").order_by("-date").annotate(
                views=Sum("video_views")
            )[:7]
            chart_data = [dict(label=i['date'], value=i['views']) for i in reversed(data)]
            return chart_data

    def __init__(self, instance=None, *args, **kwargs):
        self.ordering = {}
        if instance:
            ids = None
            if isinstance(instance, AccountCreation):
                ids = (instance.id,)
            elif isinstance(instance, QuerySet):
                ids = instance.values_list('id', flat=True).distinct()
            elif isinstance(instance, list):
                ids = [i.id for i in instance]

            if ids:
                queryset = AccountCreation.objects.filter(
                    id__in=ids
                ).values('id').order_by('id')

                order_data = queryset.annotate(
                    start=Min('campaign_creations__start'),
                    end=Max('campaign_creations__end'),
                    goal_units=Sum('campaign_creations__goal_units'),
                )
                for e in order_data:
                    self.ordering[e['id']] = e

        super(AccountCreationListSerializer, self).__init__(instance, *args, **kwargs)

        self.today = datetime.now().date()

    class Meta:
        model = AccountCreation
        fields = (
            "id", "account", "name",
            "is_optimization_active", "is_changed",
            # from the campaigns
            "start", "end", "status",
            # delivered stats
            "goal_units", 'weekly_chart', 'campaigns_count', 'ad_groups_count', 'creative_count',
            'channels_count', 'videos_count', 'keywords_count',

            'creative', 'structure', 'goal_charts',
            'is_paused', 'is_approved', 'is_ended',
        )

    @staticmethod
    def get_is_optimization_active(*_):
        return True

    @staticmethod
    def get_status(obj):
        if obj.is_ended:
            return "Ended"
        elif obj.is_paused:
            return "Paused"
        elif obj.is_approved:
            if 0:  # campaign is launched on AdWords
                return "Running"
            else:
                return "Approved"
        else:
            return "Pending"


class AccountCreationDetailsSerializer(AccountCreationListSerializer):

    campaign_creations = OptimizationCampaignsSerializer(many=True,
                                                         read_only=True)

    class Meta:
        model = AccountCreation
        fields = AccountCreationListSerializer.Meta.fields + ('campaign_creations',)


class OptimizationUpdateAccountSerializer(ModelSerializer):

    video_networks = ListField()

    class Meta:
        model = AccountCreation
        fields = (
            'name',
            'is_ended',
            'is_paused',
            'is_approved',
            'goal_type',
            'type',
            'video_ad_format',
            'bidding_type',
            'delivery_method',
            'video_networks',
        )

    def validate(self, data):
        for f in ('devices', 'video_networks', 'languages'):
            if f in data and not data[f]:
                raise ValidationError(
                    "{}: empty set is not allowed".format(f))
        if 'video_networks' in data:
            video_networks = data['video_networks']
            if AccountCreation.VIDEO_PARTNER_DISPLAY_NETWORK in video_networks and AccountCreation.YOUTUBE_VIDEO not in video_networks:
                raise ValidationError(
                    "Cannot target display network without first "
                    "targeting YouTube video network")

        if data.get("is_approved") is True:
            raise ValidationError(
                "You cannot approve account creations "
                "unless you have at least one connected MCC account"
            )

        return super(OptimizationUpdateAccountSerializer, self).validate(data)


class OptimizationCreateAccountSerializer(
        OptimizationUpdateAccountSerializer):

    class Meta:
        model = AccountCreation
        fields = OptimizationUpdateAccountSerializer.Meta.fields + ('owner',)


class OptimizationUpdateCampaignSerializer(ModelSerializer):

    devices = ListField()

    class Meta:
        model = CampaignCreation
        fields = (
            'name',
            'is_approved',
            'is_paused',
            'start',
            'end',
            'goal_units',
            'budget',
            'languages',
            'devices',
            'max_rate',
        )

    def validate(self, data):
        if "devices" in data and not data["devices"]:
            raise ValidationError("devices: empty set is not allowed")

        # approving process
        if self.instance and data.get("is_approved") is True:
            required_fields = OrderedDict([
                ("start", "start date"),
                ("end", "end date"),
                ("budget", "budget"),
                ("max_rate", "max rate"),
                ("goal_units", "goal"),
            ])
            empty_fields = [
                required_fields[f] for f in required_fields
                if not getattr(self.instance, f)
            ]
            if empty_fields:
                raise ValidationError(
                    'These fields are required for approving: '
                    '{}'.format(", ".join(empty_fields))
                )

        # if one of the following fields is provided
        if {"is_approved", "start", "end"} & set(data.keys()):
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

        return super(OptimizationUpdateCampaignSerializer,
                     self).validate(data)


class OptimizationAppendCampaignSerializer(ModelSerializer):

    class Meta:
        model = CampaignCreation
        fields = (
            'name', 'account_creation',
        )


class OptimizationCreateCampaignSerializer(
    OptimizationUpdateCampaignSerializer):
    class Meta:
        model = CampaignCreation
        fields = OptimizationUpdateCampaignSerializer.Meta.fields + (
            'account_creation', )


class OptimizationLocationRuleUpdateSerializer(ModelSerializer):
    class Meta:
        model = LocationRule


class OptimizationAdGroupUpdateSerializer(ModelSerializer):
    genders = ListField()
    parents = ListField()
    age_ranges = ListField()

    class Meta:
        model = AdGroupCreation
        exclude = ('genders_raw', 'age_ranges_raw', 'parents_raw')

    def validate(self, data):
        for f in ('genders', 'parents', 'age_ranges'):
            if f in data and not data[f]:
                raise ValidationError(
                    "{}: empty set is not allowed".format(f))

        # SAAS-158: CPv that is entered on ad group level
        # should be less than Max CPV at placement level
        if "max_rate" in data and self.instance:
            campaign_rate = self.instance.campaign_creation.max_rate
            if campaign_rate is not None:
                if Decimal(data['max_rate']) > campaign_rate:
                    raise ValidationError(
                        "Max rate at ad group level shouldn't be bigger "
                        "than the max rate at placement level"
                    )

        # approving process
        if self.instance and data.get("is_approved") is True:
            required_fields = OrderedDict([
                ("max_rate", "max CPV"),
                ("video_url", "video URL"),
                ("display_url", "display URL"),
                ("final_url", "final URL"),
            ])
            empty_fields = [
                required_fields[f] for f in required_fields
                if not getattr(self.instance, f)
            ]
            if empty_fields:
                raise ValidationError(
                    'These fields are required for approving: '
                    '{}'.format(", ".join(empty_fields))
                )

        return super(OptimizationAdGroupUpdateSerializer,
                     self).validate(data)


class OptimizationAppendAdGroupSerializer(ModelSerializer):

    class Meta:
        model = AdGroupCreation
        fields = (
            'name', 'campaign_creation',
        )


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
