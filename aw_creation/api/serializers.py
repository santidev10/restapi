import re

from django.db.models import QuerySet, Min, Max, F, Case,\
    When, Sum,\
    IntegerField as AggrIntegerField, FloatField as AggrFloatField, \
    DecimalField as AggrDecimalField
from rest_framework.serializers import ModelSerializer, \
    SerializerMethodField, ListField, \
    ValidationError

from aw_creation.models import *
from aw_reporting.models import GeoTarget


class SimpleGeoTargetSerializer(ModelSerializer):
    name = SerializerMethodField()

    @staticmethod
    def get_name(obj):
        return obj.canonical_name

    class Meta:
        model = GeoTarget
        fields = ("id", "name")


class OptimizationAdGroupSerializer(ModelSerializer):

    thumbnail = SerializerMethodField()
    targeting = SerializerMethodField()
    age_ranges = SerializerMethodField()
    genders = SerializerMethodField()
    parents = SerializerMethodField()

    @staticmethod
    def get_targeting(obj):
        targeting = {k[0]: [] for k in TargetingItem.TYPES}
        items = obj.targeting_items.all().values('type', 'criteria')
        for i in items:
            targeting[i['type']].append(i)

        for list_type, items in targeting.items():
            if len(items):
                ids = set(i['criteria'] for i in items)
                if list_type == TargetingItem.CHANNEL_TYPE:
                    info = Channel.objects.in_bulk(ids)
                    for item in items:
                        item_info = info.get(item['criteria'])
                        item['name'] = item_info.title if item_info else None
                        item['thumbnail'] = item_info.thumbnail_image_url \
                            if item_info else None

                elif list_type == TargetingItem.VIDEO_TYPE:
                    info = Video.objects.in_bulk(ids)
                    for item in items:
                        item_info = info.get(item['criteria'])
                        item['name'] = item_info.title if item_info else None
                        item['thumbnail'] = item_info.thumbnail_image_url \
                            if item_info else None

                elif list_type == TargetingItem.TOPIC_TYPE:
                    info = dict(
                        Topic.objects.filter(
                            id__in=ids).values_list('id', 'name')
                    )
                    for item in items:
                        item['name'] = info.get(int(item['criteria']))

                elif list_type == TargetingItem.INTEREST_TYPE:
                    info = dict(
                        Audience.objects.filter(
                            id__in=ids).values_list('id', 'name')
                    )
                    for item in items:
                        item['name'] = info.get(int(item['criteria']))
                elif list_type == TargetingItem.KEYWORD_TYPE:
                    for item in items:
                        item['name'] = item['criteria']
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
    ad_group_creations = OptimizationAdGroupSerializer(many=True,
                                                       read_only=True)
    languages = SerializerMethodField()
    devices = SerializerMethodField()
    location_rules = LocationRuleSerializer(many=True, read_only=True)
    ad_schedule_rules = AdScheduleSerializer(many=True, read_only=True)
    frequency_capping = FrequencyCapSerializer(many=True, read_only=True)

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
            'budget',
            'max_rate',
            'languages',
            'ad_group_creations',
            'devices',
            'location_rules',
            'frequency_capping',
            'goal_units',
            'ad_schedule_rules',
        )


class DeliverySerializerMethodField(SerializerMethodField):
    def to_representation(self, value):
        return self.parent.delivery.get(value.id, {}).get(self.field_name)


class OrderingSerializerMethodField(SerializerMethodField):
    def to_representation(self, value):
        return self.parent.ordering.get(value.id, {}).get(self.field_name)


class OptimizationAccountListSerializer(ModelSerializer):
    is_optimization_active = SerializerMethodField()
    start = OrderingSerializerMethodField()
    end = OrderingSerializerMethodField()
    ordered_cpm = OrderingSerializerMethodField()
    ordered_cpv = OrderingSerializerMethodField()
    ordered_impressions_cost = OrderingSerializerMethodField()
    ordered_views_cost = OrderingSerializerMethodField()
    ordered_impressions = OrderingSerializerMethodField()
    ordered_views = OrderingSerializerMethodField()

    cpm = DeliverySerializerMethodField()
    cpv = DeliverySerializerMethodField()
    impressions_cost = DeliverySerializerMethodField()
    views_cost = DeliverySerializerMethodField()
    impressions = DeliverySerializerMethodField()
    views = DeliverySerializerMethodField()

    def __init__(self, instance=None, *args, **kwargs):
        self.delivery = {}
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

                delivery = queryset.annotate(
                    impressions=Sum('campaign_creations__campaign__impressions'),
                    views=Sum('campaign_creations__campaign__video_views'),
                    cost=Sum('campaign_creations__campaign__cost'),
                    impressions_cost=Sum(
                        Case(
                            When(
                                goal_type=AccountCreation.GOAL_IMPRESSIONS,
                                then=F('campaign_creations__campaign__cost'),
                            ),
                            output_field=AggrIntegerField(),
                        )
                    ),
                    views_cost=Sum(
                        Case(
                            When(
                                goal_type=AccountCreation.GOAL_VIDEO_VIEWS,
                                then=F('campaign_creations__campaign__cost'),
                            ),
                            output_field=AggrIntegerField(),
                        )
                    ),
                ).annotate(
                    cpm=Case(
                        When(
                            impressions__gt=0,
                            cost__isnull=False,
                            then=F('cost') / F('impressions') * 1000,
                        ),
                        output_field=AggrFloatField(),
                    ),
                    cpv=Case(
                        When(
                            views__gt=0,
                            cost__isnull=False,
                            then=F('cost') / F('views'),
                        ),
                        output_field=AggrFloatField(),
                    ),
                )
                for e in delivery:
                    self.delivery[e['id']] = e

                order_data = queryset.annotate(
                    ordered_impressions=Sum(
                        Case(
                            When(
                                goal_type=AccountCreation.GOAL_IMPRESSIONS,
                                then=F('campaign_creations__goal_units'),
                            ),
                            output_field=AggrIntegerField(),
                        )
                    ),
                ).annotate(
                    start=Min('campaign_creations__start'),
                    end=Max('campaign_creations__end'),
                    budget=Sum('campaign_creations__budget'),
                    ordered_views=Sum(
                        Case(
                            When(
                                goal_type=AccountCreation.GOAL_VIDEO_VIEWS,
                                then=F('campaign_creations__goal_units'),
                            ),
                            output_field=AggrIntegerField(),
                        )
                    ),
                    ordered_impressions_cost=Sum(
                        Case(
                            When(
                                campaign_creations__max_rate__isnull=False,
                                campaign_creations__goal_units__isnull=False,
                                goal_type=AccountCreation.GOAL_IMPRESSIONS,
                                then=F('campaign_creations__max_rate') * F('campaign_creations__goal_units') / 1000.,
                            ),
                            output_field=AggrDecimalField(),
                        )
                    ),
                    ordered_views_cost=Sum(
                        Case(
                            When(
                                campaign_creations__max_rate__isnull=False,
                                campaign_creations__goal_units__isnull=False,
                                goal_type=AccountCreation.GOAL_VIDEO_VIEWS,
                                then=F('campaign_creations__max_rate') * F('campaign_creations__goal_units'),
                            ),
                            output_field=AggrDecimalField(),
                        )
                    ),
                ).annotate(
                    ordered_cpv=Case(
                        When(
                            ordered_views_cost__isnull=False,
                            ordered_views__isnull=False,
                            then=F('ordered_views_cost') / F('ordered_views'),
                        ),
                        output_field=AggrDecimalField(),
                    ),
                    ordered_cpm=Case(
                        When(
                            ordered_impressions_cost__isnull=False,
                            ordered_impressions__isnull=False,
                            then=F('ordered_impressions_cost') / F('ordered_impressions') * 1000,
                        ),
                        output_field=AggrDecimalField(),
                    ),
                )
                for e in order_data:
                    self.ordering[e['id']] = e

        super(OptimizationAccountListSerializer,
              self).__init__(instance, *args, **kwargs)

    class Meta:
        model = AccountCreation
        fields = (
            "id", "name", "is_ended", "is_paused",
            "is_optimization_active", "is_changed", "is_approved",
            # from the campaigns
            "start", "end",
            # plan stats
            "ordered_cpm", "ordered_cpv", "ordered_impressions",
            "ordered_impressions_cost", "ordered_views",
            "ordered_views_cost",
            # delivered stats
            "cpm", "cpv", "impressions",
            "impressions_cost", "views",
            "views_cost",
        )

    @staticmethod
    def get_is_optimization_active(*_):
        return True


class OptimizationAccountDetailsSerializer(
        OptimizationAccountListSerializer):

    budget = OrderingSerializerMethodField()
    campaign_creations = OptimizationCampaignsSerializer(many=True,
                                                         read_only=True)
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

    class Meta:
        model = AccountCreation
        fields = OptimizationAccountListSerializer.Meta.fields + (
            'goal_type',
            'budget',
            'video_networks',
            'bidding_type',
            'type',
            'video_ad_format',
            'delivery_method',
            'campaign_creations',
        )


class OptimizationUpdateAccountSerializer(ModelSerializer):

    video_networks = ListField()

    class Meta:
        model = AccountCreation
        fields = (
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

        return super(OptimizationUpdateAccountSerializer, self).validate(data)


class OptimizationCreateAccountSerializer(
        OptimizationUpdateAccountSerializer):

    class Meta:
        model = AccountCreation
        fields = OptimizationUpdateAccountSerializer.Meta.fields + (
            'owner',)


class OptimizationUpdateCampaignSerializer(ModelSerializer):

    devices = ListField()

    class Meta:
        model = CampaignCreation
        fields = (
            'is_approved',
            'is_paused',
            'start',
            'end',
            'goal_units',
            'budget',
            'languages',
            'devices',
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
        return super(OptimizationAdGroupUpdateSerializer,
                     self).validate(data)
