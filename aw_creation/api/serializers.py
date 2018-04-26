import json
import logging
from collections import defaultdict

from django.db.models import Min, Max, Sum, Count
from rest_framework.serializers import ModelSerializer, SerializerMethodField, \
    ListField, ValidationError, BooleanField, DictField

from aw_creation.models import TargetingItem, AdGroupCreation, \
    CampaignCreation, AccountCreation, LocationRule, AdScheduleRule, \
    FrequencyCap, AdCreation
from aw_reporting.models import GeoTarget, Topic, Audience, AdGroupStatistic, \
    Campaign, base_stats_aggregate, dict_norm_base_stats, \
    dict_calculate_stats, ConcatAggregate, VideoCreativeStatistic, Ad, \
    Opportunity, goal_type_str
from singledb.connector import SingleDatabaseApiConnector, \
    SingleDatabaseApiConnectorException

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

    @staticmethod
    def get_ad_creations(obj):
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

    @staticmethod
    def get_ad_group_creations(obj):
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


class StatField(SerializerMethodField):
    def to_representation(self, value):
        return self.parent.stats.get(value.id, {}).get(self.field_name)


class StruckField(SerializerMethodField):
    def to_representation(self, value):
        return self.parent.struck.get(value.id, {}).get(self.field_name)


class AccountCreationListSerializer(ModelSerializer):
    is_changed = BooleanField()
    name = SerializerMethodField()
    thumbnail = SerializerMethodField()
    weekly_chart = SerializerMethodField()
    start = SerializerMethodField()
    end = SerializerMethodField()
    is_disapproved = SerializerMethodField()
    from_aw = SerializerMethodField()
    status = SerializerMethodField()
    goal_type = SerializerMethodField()
    updated_at = SerializerMethodField()
    # analytic data
    impressions = StatField()
    video_views = StatField()
    cost = StatField()
    clicks = StatField()
    video_view_rate = StatField()
    ctr_v = StatField()
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

    class Meta:
        model = AccountCreation
        fields = (
            "id", "name", "start", "end", "account", "status", "is_managed",
            "thumbnail", "is_changed", "weekly_chart",
            # delivered stats
            "clicks", "cost", "impressions", "video_views", "video_view_rate",
            "ctr_v", "ad_count", "channel_count", "video_count",
            "interest_count", "topic_count", "keyword_count", "is_disapproved",
            "updated_at", "brand", "agency", "from_aw", "goal_type")

    def __init__(self, *args, **kwargs):
        super(AccountCreationListSerializer, self).__init__(*args, **kwargs)
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
            data = Campaign.objects.filter(
                account__account_creations__id__in=ids
            ).values('account__account_creations__id').order_by(
                'account__account_creations__id').annotate(
                start=Min("start_date"),
                end=Max("end_date"),
                **base_stats_aggregate
            )
            for i in data:
                dict_norm_base_stats(i)
                dict_calculate_stats(i)
                self.stats[i['account__account_creations__id']] = i
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
        if obj.account is None:
            return None
        return obj.account.update_time

    def get_brand(self, obj: AccountCreation):
        opportunity = self._get_opportunity(obj)
        return opportunity.brand\
            if opportunity is not None and self.is_chf else None

    def get_agency(self, obj):
        opportunity = self._get_opportunity(obj)
        if opportunity is None or opportunity.agency is None:
            return None
        return opportunity.agency.name if self.is_chf else None

    def get_goal_type(self, obj):
        if not self.is_chf:
            return None
        opportunity = self._get_opportunity(obj)
        if not opportunity:
            return None
        goal_type_ids = opportunity.placements.filter(
            goal_type_id__isnull=False).values_list("goal_type_id", flat=True)
        return ", ".join(
            [goal_type_str(goal_type_id) for goal_type_id in goal_type_ids])

    def _get_opportunity(self, obj):
        return Opportunity.objects.filter(
            placements__adwords_campaigns__campaign_creation__account_creation=obj).first()


class AccountCreationSetupSerializer(ModelSerializer):
    campaign_creations = SerializerMethodField()

    @staticmethod
    def get_campaign_creations(obj):
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
            'name', 'start', 'end', 'budget',
            'languages', 'devices',
            'delivery_method', 'video_networks', 'content_exclusions',
        )

    def validate_start(self, value):
        if value and value < self.instance.account_creation.get_today_date() and value != self.instance.start:
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
