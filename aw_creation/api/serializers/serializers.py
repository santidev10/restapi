import json
import logging

from drf_yasg.utils import swagger_serializer_method
from rest_framework.serializers import ModelSerializer, SerializerMethodField, \
    ListField, ValidationError, DictField

from aw_creation.models import TargetingItem, AdGroupCreation, \
    CampaignCreation, AccountCreation, LocationRule, AdScheduleRule, \
    FrequencyCap, AdCreation
from aw_reporting.models import GeoTarget, Topic, Audience
from singledb.connector import SingleDatabaseApiConnector, \
    SingleDatabaseApiConnectorException
from utils.datetime import now_in_default_tz

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
                items = connector.get_channels_base_info(ids)
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
                items = connector.get_videos_base_info(ids)
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


class AdCreationSetupSerializer(ModelSerializer):
    video_ad_format = SerializerMethodField()
    is_disapproved = SerializerMethodField()

    @staticmethod
    def get_video_ad_format(obj):
        item_id = obj.ad_group_creation.video_ad_format
        options = dict(obj.ad_group_creation.__class__.VIDEO_AD_FORMATS)
        return dict(id=item_id, name=options[item_id])

    @staticmethod
    def get_is_disapproved(obj):
        return obj.ad.is_disapproved if obj.ad is not None else False

    class Meta:
        model = AdCreation
        fields = (
            'id', 'name', 'updated_at', 'companion_banner',
            'final_url', 'video_url', 'display_url',
            'tracking_template', 'custom_params', 'video_ad_format',
            'video_id', 'video_title', 'video_description', 'video_thumbnail',
            'video_channel_title', 'video_duration',

            "beacon_impression_1", "beacon_impression_2", "beacon_impression_3",
            "beacon_view_1", "beacon_view_2", "beacon_view_3",
            "beacon_skip_1", "beacon_skip_2", "beacon_skip_3",
            "beacon_first_quartile_1", "beacon_first_quartile_2",
            "beacon_first_quartile_3",
            "beacon_midpoint_1", "beacon_midpoint_2", "beacon_midpoint_3",
            "beacon_third_quartile_1", "beacon_third_quartile_2",
            "beacon_third_quartile_3",
            "beacon_completed_1", "beacon_completed_2", "beacon_completed_3",
            "beacon_vast_1", "beacon_vast_2", "beacon_vast_3",
            "beacon_dcm_1", "beacon_dcm_2", "beacon_dcm_3", "is_disapproved"
        )


class AdGroupCreationSetupSerializer(ModelSerializer):
    targeting = SerializerMethodField()
    age_ranges = SerializerMethodField()
    genders = SerializerMethodField()
    parents = SerializerMethodField()
    video_ad_format = SerializerMethodField()
    ad_creations = SerializerMethodField()

    @swagger_serializer_method(serializer_or_field=AdCreationSetupSerializer(many=True))
    def get_ad_creations(self, obj):
        queryset = obj.ad_creations.filter(is_deleted=False)
        ad_creations = AdCreationSetupSerializer(queryset, many=True).data
        return ad_creations

    @staticmethod
    def get_video_ad_format(obj):
        item_id = obj.video_ad_format
        options = dict(obj.__class__.VIDEO_AD_FORMATS)
        return dict(id=item_id, name=options[item_id])

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
    def get_targeting(obj):
        items = obj.targeting_items.all() \
            .values('type', 'criteria', 'is_negative')

        for t_type, _ in TargetingItem.TYPES:
            t_items = list(filter(lambda e: e['type'] == t_type, items))
            if t_items:
                add_targeting_list_items_info(t_items, t_type)

        targeting = {k[0]: {"positive": [], "negative": []}
                     for k in TargetingItem.TYPES}
        for item in items:
            key = "negative" if item['is_negative'] else "positive"
            targeting[item['type']][key].append(item)

        return targeting

    class Meta:
        model = AdGroupCreation
        fields = (
            'id', 'name', 'updated_at', 'max_rate',
            'age_ranges', 'genders', 'parents', 'targeting',
            'ad_creations', 'video_ad_format',
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
        fields = '__all__'


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


class CampaignCreationSetupSerializer(ModelSerializer):
    location_rules = LocationRuleSerializer(many=True, read_only=True)
    ad_schedule_rules = AdScheduleSerializer(many=True, read_only=True)
    frequency_capping = FrequencyCapSerializer(many=True, read_only=True)

    languages = SerializerMethodField()
    devices = SerializerMethodField()
    type = SerializerMethodField()
    delivery_method = SerializerMethodField()
    video_networks = SerializerMethodField()
    content_exclusions = SerializerMethodField()
    ad_group_creations = SerializerMethodField()

    @swagger_serializer_method(serializer_or_field=AdGroupCreationSetupSerializer(many=True))
    def get_ad_group_creations(self, obj):
        queryset = obj.ad_group_creations.filter(is_deleted=False)
        ad_group_creations = AdGroupCreationSetupSerializer(queryset, many=True)
        return ad_group_creations.data

    @staticmethod
    def get_content_exclusions(obj):
        content_exclusions = [
            dict(id=uid, name=n)
            for uid, n in CampaignCreation.CONTENT_LABELS
            if uid in obj.content_exclusions
        ]
        return content_exclusions

    @staticmethod
    def get_type(obj):
        item_id = obj.type
        options = dict(obj.__class__.CAMPAIGN_TYPES)
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
            'devices', 'location_rules', 'frequency_capping',
            'ad_schedule_rules',
            'video_networks', 'delivery_method', 'type',
            'content_exclusions',
            'ad_group_creations'
        )


class AccountCreationSetupSerializer(ModelSerializer):
    campaign_creations = SerializerMethodField()

    @swagger_serializer_method(serializer_or_field=CampaignCreationSetupSerializer(many=True))
    def get_campaign_creations(self, obj):
        queryset = obj.campaign_creations.filter(is_deleted=False)
        campaign_creations = CampaignCreationSetupSerializer(queryset,
                                                             many=True).data
        return campaign_creations

    class Meta:
        model = AccountCreation
        fields = (
            'id', 'name', 'account', 'is_ended', 'is_approved', 'is_paused',
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
    content_exclusions = ListField()

    class Meta:
        model = CampaignCreation
        fields = (
            "budget",
            "budget_type",
            "content_exclusions",
            "delivery_method",
            "devices",
            "end",
            "languages",
            "name",
            "start",
            "video_networks",
        )

    def validate_start(self, value):
        if value == self.instance.start:
            return value

        today = self.instance.account_creation.get_today_date()
        is_running = self.instance.is_pulled_to_aw \
                     and (self.instance.start is None
                          or self.instance.start <= today)
        if is_running:
            raise ValidationError(
                "Start date may not be edited for active campaign")

        if value and value < today:
            raise ValidationError("This date is in the past")

        return value

    def validate_end(self, value):
        if value and value < self.instance.account_creation.get_today_date() and value != self.instance.end:
            raise ValidationError("This date is in the past")
        return value

    def validate(self, data):
        for f in (
                'devices', 'video_networks', 'languages', 'genders', 'parents',
                'age_ranges'):
            if f in data and not data[f]:
                raise ValidationError(
                    "{}: empty set is not allowed".format(f))

        if 'video_networks' in data:
            video_networks = data['video_networks']
            if CampaignCreation.VIDEO_PARTNER_DISPLAY_NETWORK in video_networks and \
                    CampaignCreation.YOUTUBE_VIDEO not in video_networks:
                raise ValidationError(
                    "Cannot target display network without first "
                    "targeting YouTube video network")

        # if one of the following fields is provided
        if {"start", "end"} & set(data.keys()):
            today = now_in_default_tz().date()
            start, end = None, None
            if self.instance:
                start, end = self.instance.start, self.instance.end

            if data.get("start"):
                start = data.get("start")
            if data.get("end"):
                end = data.get("end")

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
        fields = "__all__"


class AdGroupCreationUpdateSerializer(ModelSerializer):
    genders = ListField()
    parents = ListField()
    age_ranges = ListField()
    targeting = DictField()

    def update(self, instance, validated_data):
        instance = super(AdGroupCreationUpdateSerializer, self).update(instance,
                                                                       validated_data)

        targeting = validated_data.get("targeting")
        if targeting:
            bulk_items = []
            for list_type, item_lists in targeting.items():
                for list_key, item_ids in item_lists.items():
                    kwargs = dict(
                        ad_group_creation=instance,
                        type=list_type,
                        is_negative=list_key == "negative",
                    )
                    queryset = TargetingItem.objects.filter(**kwargs)
                    # delete items not in the list
                    queryset.exclude(criteria__in=item_ids).delete()

                    # insert new items
                    existed_ids = queryset.values_list("criteria", flat=True)
                    to_insert_ids = set(item_ids) - set(existed_ids)
                    if to_insert_ids:
                        bulk_items.extend(
                            TargetingItem(criteria=uid, **kwargs) for uid in
                            to_insert_ids
                        )

            if bulk_items:
                TargetingItem.objects.bulk_create(bulk_items)

        return instance

    @staticmethod
    def validate_targeting(value):
        allowed_keys = set(t for t, _ in TargetingItem.TYPES)
        error_text = 'Targeting items must be sent in the following format: ' \
                     '{"keyword": {"positive": ["spam", "ham"], "negative": ["adult films"]}, ...}.\n' \
                     'Allowed keys are %s' % allowed_keys
        unknown_keys = value.keys() - allowed_keys
        if unknown_keys:
            raise ValidationError(error_text)
        second_lvl_keys = {"positive", "negative"}
        for v in value.values():
            if not isinstance(v, dict) or set(v.keys()) != second_lvl_keys:
                raise ValidationError(error_text)
            for lists in v.values():
                if not isinstance(lists, list):
                    raise ValidationError(error_text)

        count = sum(len(item_ids) for item_lists in value.values()
                    for item_ids in item_lists.values())

        limit = 20000
        if count > limit:
            raise ValidationError(
                "Too many targeting items in this ad group: {:,}. You are allowed to use up to {:,}"
                " targeting items per ad group".format(count, limit))
        return value

    def validate(self, data):
        for f in ('genders', 'parents', 'age_ranges'):
            if f in data and not data[f]:
                raise ValidationError("{}: empty set is not allowed".format(f))

        return super(AdGroupCreationUpdateSerializer, self).validate(data)

    class Meta:
        model = AdGroupCreation
        exclude = (
            'genders_raw', 'age_ranges_raw', 'parents_raw', 'campaign_creation')


class AppendAdGroupCreationSetupSerializer(ModelSerializer):
    class Meta:
        model = AdGroupCreation
        fields = (
            'name', 'campaign_creation', 'genders_raw', 'age_ranges_raw',
            'parents_raw',
        )


class AdCreationUpdateSerializer(ModelSerializer):
    custom_params = ListField()

    class Meta:
        model = AdCreation
        exclude = ('ad_group_creation',)

    @staticmethod
    def validate_custom_params(custom_params):
        if isinstance(custom_params, list) and len(custom_params) == 1 \
                and isinstance(custom_params[0], str) and custom_params[
            0].startswith("["):
            custom_params = json.loads(custom_params[0])

        if len(custom_params) > 3:
            raise ValidationError(
                'You cannot use more than 3 custom parameters'
            )
        keys = {"name", "value"}
        for i in custom_params:
            if type(i) is not dict or set(i.keys()) != keys:
                # all(ord(c) < 128 for c in test)  test.isalpha()
                raise ValidationError(
                    'Custom parameters format is [{"name": "ad", "value": "demo"}, ..]'
                )
            if not (i["name"].isalnum() and all(
                    ord(c) < 128 for c in i["name"])):
                raise ValidationError(
                    'Invalid character in custom parameter key'
                )
            if " " in i['value']:
                raise ValidationError(
                    'Invalid character in custom parameter value'
                )
        return custom_params

    def save(self, **kwargs):
        for f in AdCreation.tag_field_names:
            if f in self.validated_data:
                value = self.validated_data[f]
                prev_value = getattr(self.instance, f)
                if value != prev_value:
                    self.validated_data["{}_changed".format(f)] = True
        result = super(AdCreationUpdateSerializer, self).save(**kwargs)
        return result


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


class UpdateTargetingDirectionSerializer(ModelSerializer):
    class Meta:
        model = TargetingItem
        fields = ('is_negative',)


class AdGroupTargetingListSerializer(ModelSerializer):
    class Meta:
        model = TargetingItem
        exclude = ('type', 'id', 'ad_group_creation')


class AdGroupTargetingListUpdateSerializer(ModelSerializer):
    class Meta:
        model = TargetingItem
        exclude = ('id',)
