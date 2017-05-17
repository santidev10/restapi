import calendar
import json
import logging
import uuid
from decimal import Decimal

from django.core.validators import MaxValueValidator, MinValueValidator, \
    RegexValidator
from django.db import models

logger = logging.getLogger(__name__)


BULK_CREATE_CAMPAIGNS_COUNT = 5
BULK_CREATE_AD_GROUPS_COUNT = 5

WEEKDAYS = list(calendar.day_name)
NameValidator = RegexValidator(r"^[^#']*$", 'Not allowed characters')
YT_VIDEO_REGEX = r"^(?:https?:/{2})?(?:w{3}\.)?youtu(?:be)?\.(?:com|be)"\
                 r"(?:/watch\?v=|/video/)([^\s&]+)$"
VideoUrlValidator = RegexValidator(YT_VIDEO_REGEX, 'Wrong video url')


def get_uid(length=12):
    return str(uuid.uuid4()).replace('-', '')[:length]


def get_version():
    return get_uid(8)


class UniqueItem(models.Model):

    name = models.CharField(max_length=250, validators=[NameValidator])

    class Meta:
        abstract = True

    def __str__(self):
        return self.unique_name

    @property
    def unique_name(self):
        return "{} #{}".format(self.name, self.id)


class AccountCreation(UniqueItem):
    id = models.CharField(primary_key=True, max_length=12,
                          default=get_uid, editable=False)
    owner = models.ForeignKey('userprofile.userprofile',
                              related_name="aw_account_creations")
    aw_manager_id = models.CharField(
        max_length=15, null=True,  blank=True,
    )
    account = models.OneToOneField(
        "aw_reporting.Account", related_name='account_creation',
        on_delete=models.SET_NULL, null=True, blank=True,
    )
    is_paused = models.BooleanField(default=False)
    is_ended = models.BooleanField(default=False)
    is_approved = models.BooleanField(default=False)
    is_changed = models.BooleanField(default=True)
    version = models.CharField(max_length=8, default=get_version)
    created_at = models.DateTimeField(auto_now_add=True)

    # new common fields that were moved up to the account level
    VIDEO_TYPE = 'VIDEO'
    DISPLAY_TYPE = 'DISPLAY'
    CAMPAIGN_TYPES = ((VIDEO_TYPE, "Video"),
                      (DISPLAY_TYPE, "Display"))
    type = models.CharField(
        max_length=15,
        choices=CAMPAIGN_TYPES,
        default=VIDEO_TYPE,
    )

    GOAL_IMPRESSIONS = "GOAL_IMPRESSIONS"
    GOAL_VIDEO_VIEWS = "GOAL_VIDEO_VIEWS"
    GOAL_TYPES = (
        (GOAL_VIDEO_VIEWS, "Views"),
        (GOAL_IMPRESSIONS, "Impressions"),
    )
    goal_type = models.CharField(
        max_length=20,
        choices=GOAL_TYPES,
        default=GOAL_VIDEO_VIEWS,
    )

    STANDARD_DELIVERY = 'STANDARD'
    ACCELERATED_DELIVERY = 'ACCELERATED'
    DELIVERY_METHODS = (
        (STANDARD_DELIVERY, "Standard"),
        (ACCELERATED_DELIVERY, "Accelerated"),
    )
    delivery_method = models.CharField(
        max_length=15,
        choices=DELIVERY_METHODS,
        default=STANDARD_DELIVERY,
    )

    IN_STREAM_TYPE = 'TRUE_VIEW_IN_STREAM'
    DISCOVERY_TYPE = 'TRUE_VIEW_IN_DISPLAY'
    BUMPER_AD = 'BUMPER'
    VIDEO_AD_FORMATS = (
        (IN_STREAM_TYPE, "In-stream"),
        (DISCOVERY_TYPE, "Discovery"),
        (BUMPER_AD, "Bumper"),
    )
    video_ad_format = models.CharField(
        max_length=20,
        choices=VIDEO_AD_FORMATS,
        default=IN_STREAM_TYPE,
    )

    MANUAL_CPV_BIDDING = 'MANUAL_CPV'
    BIDDING_TYPES = (
        (MANUAL_CPV_BIDDING, "Manual CPV"),
    )
    bidding_type = models.CharField(
        max_length=20,
        choices=BIDDING_TYPES,
        default=MANUAL_CPV_BIDDING,
    )

    YOUTUBE_SEARCH = 'YOUTUBE_SEARCH'
    YOUTUBE_VIDEO = 'YOUTUBE_VIDEO'
    VIDEO_PARTNER_DISPLAY_NETWORK = 'VIDEO_PARTNER_ON_THE_DISPLAY_NETWORK'
    VIDEO_NETWORKS = (
        (YOUTUBE_SEARCH, "Youtube search"),
        (YOUTUBE_VIDEO, "Youtube video"),
        (VIDEO_PARTNER_DISPLAY_NETWORK, "Partner display network"),
    )
    video_networks_raw = models.CharField(
        max_length=100,
        default=json.dumps(
            [YOUTUBE_SEARCH, YOUTUBE_VIDEO, VIDEO_PARTNER_DISPLAY_NETWORK]
        ),
    )
    # Cannot target display network
    # without first targeting YouTube video network

    def get_video_networks(self):
        return json.loads(self.video_networks_raw)

    def set_video_networks(self, value):
        self.video_networks_raw = json.dumps(value)
    video_networks = property(get_video_networks, set_video_networks)

    def get_aws_code(self):
        if self.account_id:
            lines = []
            for c in self.campaign_managements.filter(is_approved=True):
                lines.append(c.get_aws_code())
            lines.append(
                "sendChangesStatus('{}', '{}');".format(
                    self.account_id, self.version)
            )
            return " ".join(lines)


def default_languages():
    return Language.objects.filter(pk__in=(1000, 1003))


class Language(models.Model):
    name = models.CharField(max_length=50)
    code = models.CharField(max_length=5)


class CampaignCreation(UniqueItem):

    account_creation = models.ForeignKey(
        AccountCreation, related_name="campaign_creations",
    )
    campaign = models.OneToOneField(
        "aw_reporting.Campaign", related_name='campaign_creation',
        on_delete=models.SET_NULL, null=True, blank=True,
    )

    # fields
    start = models.DateField(null=True, blank=True)
    end = models.DateField(null=True, blank=True)
    goal_units = models.PositiveIntegerField(
        null=True, blank=True,
        validators=[MinValueValidator(1)],
    )

    max_rate = models.DecimalField(
        null=True, blank=True, max_digits=6, decimal_places=3,
    )
    budget = models.DecimalField(
        null=True, blank=True, max_digits=10, decimal_places=2,
    )
    is_paused = models.BooleanField(default=False)
    is_approved = models.BooleanField(default=False)

    languages = models.ManyToManyField(
        'Language', related_name='campaigns', default=default_languages)

    DESKTOP_DEVICE = 'DESKTOP_DEVICE'
    MOBILE_DEVICE = 'MOBILE_DEVICE'
    TABLET_DEVICE = 'TABLET_DEVICE'
    DEVICES = (
        (DESKTOP_DEVICE, "Desktop"),
        (MOBILE_DEVICE, "Mobile"),
        (TABLET_DEVICE, "Tablet"),
    )
    devices_raw = models.CharField(
        max_length=100,
        default=json.dumps(
            [DESKTOP_DEVICE, MOBILE_DEVICE, TABLET_DEVICE]
        ),
    )

    def get_devices(self):
        return json.loads(self.devices_raw)

    def set_devices(self, value):
        self.devices_raw = json.dumps(value)
    devices = property(get_devices, set_devices)

    @property
    def campaign_is_paused(self):
        return (self.is_paused or self.account_management.is_paused or
                self.account_management.is_ended)

    def get_aws_code(self):

        lines = [
            "var campaign = createOrUpdateCampaign('{}', '{}', {}, '{}', "
            "'{}', {}, '{}', '{}', {}, {}, {}, {}, {}, {}, {});".format(
                self.id,
                self.unique_name,
                self.budget,
                self.start.strftime("%Y-%m-%d"),
                "cpm"
                if self.video_ad_format == CampaignCreation.BUMPER_AD
                else "cpv",
                'true' if self.campaign_is_paused else 'false',
                self.start.strftime("%Y%m%d"),
                self.end.strftime("%Y%m%d"),
                self.video_networks,
                list(self.languages.values_list('id', flat=True)),
                self.devices,
                [
                    "{} {} {} {} {}".format(
                        WEEKDAYS[s.day - 1].upper(), s.from_hour,
                        s.from_minute, s.to_hour, s.to_minute
                    ) for s in self.ad_schedule_rules.all()
                ],
                {
                    f["event_type"]:f
                    for f in self.frequency_capping.all().values(
                        "event_type", "level", "time_unit", "limit"
                    )
                },
                list(
                    self.location_rules.filter(
                        geo_target_id__isnull=False
                    ).values_list("geo_target_id", flat=True)
                ),
                [
                    " ".join(
                        ("{}".format(l.latitude).rstrip('0'),
                         "{}".format(l.longitude).rstrip('0'),
                         str(l.radius), l.radius_units)
                    ) for l in self.location_rules.filter(
                        radius__gte=0, latitude__isnull=False,
                        longitude__isnull=False)
                ],
            )
        ]
        for ag in self.ad_group_managements.filter(is_approved=True):
            code = ag.get_aws_code()
            if code:
                lines.append(code)
        return " ".join(lines)


class AdGroupCreation(UniqueItem):

    campaign_creation = models.ForeignKey(
        CampaignCreation, related_name="ad_group_creations",
    )
    ad_group = models.OneToOneField(
        "aw_reporting.AdGroup", related_name='ad_group_creation',
        on_delete=models.SET_NULL, null=True, blank=True,
    )
    max_rate = models.DecimalField(null=True, blank=True,
                                   max_digits=6, decimal_places=3)
    video_url = models.URLField(validators=[VideoUrlValidator])
    display_url = models.CharField(max_length=200, blank=True, null=True)
    final_url = models.URLField(blank=True, null=True)
    ct_overlay_text = models.CharField(max_length=250,
                                       blank=True, null=True)

    is_approved = models.BooleanField(default=False)

    GENDER_FEMALE = "GENDER_FEMALE"
    GENDER_MALE = "GENDER_MALE"
    GENDER_UNDETERMINED = "GENDER_UNDETERMINED"
    GENDERS = (
        (GENDER_FEMALE, "Female"),
        (GENDER_MALE, "Male"),
        (GENDER_UNDETERMINED, "Undetermined"),
    )
    genders_raw = models.CharField(
        max_length=100,
        default=json.dumps(
            [GENDER_FEMALE, GENDER_MALE, GENDER_UNDETERMINED]
        )
    )

    def get_genders(self):
        return json.loads(self.genders_raw)

    def set_genders(self, value):
        self.genders_raw = json.dumps(value)
    genders = property(get_genders, set_genders)

    PARENT_PARENT = "PARENT_PARENT"
    PARENT_NOT_A_PARENT = "PARENT_NOT_A_PARENT"
    PARENT_UNDETERMINED = "PARENT_UNDETERMINED"
    PARENTS = (
        (PARENT_PARENT, "Parent"),
        (PARENT_NOT_A_PARENT, "Not a parent"),
        (PARENT_UNDETERMINED, "Undetermined"),
    )
    parents_raw = models.CharField(
        max_length=100,
        default=json.dumps(
            [PARENT_PARENT, PARENT_NOT_A_PARENT, PARENT_UNDETERMINED]
        )
    )

    def get_parent(self):
        return json.loads(self.parents_raw)

    def set_parent(self, value):
        self.parents_raw = json.dumps(value)
    parents = property(get_parent, set_parent)

    AGE_RANGE_18_24 = "AGE_RANGE_18_24"
    AGE_RANGE_25_34 = "AGE_RANGE_25_34"
    AGE_RANGE_35_44 = "AGE_RANGE_35_44"
    AGE_RANGE_45_54 = "AGE_RANGE_45_54"
    AGE_RANGE_55_64 = "AGE_RANGE_55_64"
    AGE_RANGE_65_UP = "AGE_RANGE_65_UP"
    AGE_RANGE_UNDETERMINED = "AGE_RANGE_UNDETERMINED"
    AGE_RANGES = (
        (AGE_RANGE_18_24, "18-24"),
        (AGE_RANGE_25_34, "25-34"),
        (AGE_RANGE_35_44, "35-44"),
        (AGE_RANGE_45_54, "45-54"),
        (AGE_RANGE_55_64, "55-64"),
        (AGE_RANGE_65_UP, "65+"),
        (AGE_RANGE_UNDETERMINED, "Undetermined"),
    )
    age_ranges_raw = models.CharField(
        max_length=200,
        default=json.dumps(
            [AGE_RANGE_18_24, AGE_RANGE_25_34, AGE_RANGE_35_44,
             AGE_RANGE_45_54, AGE_RANGE_55_64, AGE_RANGE_65_UP,
             AGE_RANGE_UNDETERMINED]
        )
    )

    def get_age_ranges(self):
        return json.loads(self.age_ranges_raw)

    def set_age_ranges(self, value):
        self.age_ranges_raw = json.dumps(value)
    age_ranges = property(get_age_ranges, set_age_ranges)

    def get_aws_code(self):
        """
        "campaign" variable have to be defined above
        :return:
        """
        targeting = self.targeting_items.all()
        channels = targeting.filter(type=TargetingItem.CHANNEL_TYPE)
        videos = targeting.filter(type=TargetingItem.VIDEO_TYPE)
        topics = targeting.filter(type=TargetingItem.TOPIC_TYPE)
        interests = targeting.filter(type=TargetingItem.INTEREST_TYPE)
        keywords = targeting.filter(type=TargetingItem.KEYWORD_TYPE)

        def qs_to_list(qs, to_int=False):
            values = qs.values_list('criteria', flat=True)
            if to_int:
                values = [int(i) for i in values]
            return values

        kwargs = (
            self.max_rate,
            self.genders,
            self.parents,
            self.age_ranges,
            qs_to_list(channels.filter(is_negative=False)),
            qs_to_list(channels.filter(is_negative=True)),
            qs_to_list(videos.filter(is_negative=False)),
            qs_to_list(videos.filter(is_negative=True)),
            qs_to_list(topics.filter(is_negative=False), to_int=True),
            qs_to_list(topics.filter(is_negative=True), to_int=True),
            qs_to_list(interests.filter(is_negative=False), to_int=True),
            qs_to_list(interests.filter(is_negative=True), to_int=True),
            qs_to_list(keywords.filter(is_negative=False)),
            qs_to_list(keywords.filter(is_negative=True)),
        )
        lines = [
            "var ad_group = createOrUpdateAdGroup(campaign, "
            "'{}', '{}', 'VIDEO_{}', {});".format(
                self.id,
                self.unique_name,
                self.campaign_management.video_ad_format,
                ", ".join(str(i) for i in kwargs)
            ),
            "createOrUpdateVideoAd(ad_group, '{}', '{}', '{}');".format(
                self.video_url,
                self.display_url if self.display_url else "",
                self.final_url if self.final_url else "",
            ),
        ]
        return " ".join(lines)


class LocationRule(models.Model):
    campaign_creation = models.ForeignKey(
        CampaignCreation,
        related_name="location_rules",
    )
    geo_target = models.ForeignKey('aw_reporting.GeoTarget', null=True)
    latitude = models.DecimalField(max_digits=11, decimal_places=8,
                                   null=True, blank=True)
    longitude = models.DecimalField(max_digits=11, decimal_places=8,
                                    null=True, blank=True)
    # 800mi and 500km
    radius = models.PositiveSmallIntegerField(
        null=True, blank=True,
        validators=[MaxValueValidator(800)],
    )
    MILE_UNITS = "MILES"
    KM_UNITS = "KILOMETERS"
    UNITS = (
        (MILE_UNITS, "Miles"),
        (KM_UNITS, "Kilometers"),
    )
    radius_units = models.CharField(
        max_length=10,
        choices=UNITS,
        default=MILE_UNITS,
    )
    bid_modifier = models.DecimalField(
        max_digits=3,
        decimal_places=2,
        default=Decimal("1"),
        validators=[MinValueValidator(Decimal("-1")),
                    MaxValueValidator(Decimal("4.0"))]
    )


class FrequencyCap(models.Model):
    campaign_creation = models.ForeignKey(
        CampaignCreation,
        related_name="frequency_capping",
    )
    IMPRESSION_TYPE = "IMPRESSION"
    VIDEO_VIEW_TYPE = "VIDEO_VIEW"
    EVENT_TYPES = ((IMPRESSION_TYPE, "Impression"),
                   (VIDEO_VIEW_TYPE, "View"))
    event_type = models.CharField(max_length=10, choices=EVENT_TYPES,
                                  default=IMPRESSION_TYPE)

    CAMPAIGN_LVL = "CAMPAIGN"
    AD_GROUP_LVL = "AD_GROUP"
    AD_GROUP_CREATIVE_LVL = "AD_GROUP_CREATIVE"
    LEVELS = (
        (CAMPAIGN_LVL, "Campaign"),
        (AD_GROUP_LVL, "AdGroup"),
        (AD_GROUP_CREATIVE_LVL, "Creative"),
    )
    level = models.CharField(max_length=20, choices=LEVELS,
                             default=CAMPAIGN_LVL)
    limit = models.PositiveSmallIntegerField(
        validators=[MinValueValidator(1)],
    )
    DAY_TIME_UNIT = "DAY"
    WEEK_TIME_UNIT = "WEEK"
    MONTH_TIME_UNIT = "MONTH"
    TIME_UNITS = (
        (DAY_TIME_UNIT, "Day"),
        (WEEK_TIME_UNIT, "Week"),
        (MONTH_TIME_UNIT, "Month"),
    )
    time_unit = models.CharField(max_length=5, choices=TIME_UNITS,
                                 default=DAY_TIME_UNIT)

    class Meta:
        unique_together = (("campaign_creation", "event_type"),)


class AdScheduleRule(models.Model):
    campaign_creation = models.ForeignKey(
        CampaignCreation,
        related_name="ad_schedule_rules",
    )
    day = models.PositiveSmallIntegerField()
    from_hour = models.PositiveSmallIntegerField()
    from_minute = models.PositiveSmallIntegerField(default=0)
    to_hour = models.PositiveSmallIntegerField()
    to_minute = models.PositiveSmallIntegerField(default=0)

    def __str__(self):
        return "{}: {}, {}:{}-{}:{}".format(
            self.campaign_creation, self.day, self.from_hour,
            self.from_minute, self.to_hour, self.to_minute
        )

    class Meta:
        unique_together = (
            ("campaign_creation", "day", "from_hour",
             "from_minute", "to_hour", "to_minute"),
        )


class TargetingItem(models.Model):
    criteria = models.CharField(max_length=150)
    ad_group_creation = models.ForeignKey(
        AdGroupCreation, related_name="targeting_items"
    )
    CHANNEL_TYPE = "channel"
    VIDEO_TYPE = "video"
    TOPIC_TYPE = "topic"
    INTEREST_TYPE = "interest"
    KEYWORD_TYPE = "keyword"
    TYPES = (
        (CHANNEL_TYPE, CHANNEL_TYPE),
        (VIDEO_TYPE, VIDEO_TYPE),
        (TOPIC_TYPE, TOPIC_TYPE),
        (INTEREST_TYPE, INTEREST_TYPE),
        (KEYWORD_TYPE, KEYWORD_TYPE),
    )
    type = models.CharField(max_length=20, choices=TYPES)
    is_negative = models.BooleanField(default=False)

    class Meta:
        unique_together = (('ad_group_creation', 'type', 'criteria'),)
        ordering = ['ad_group_creation', 'type', 'is_negative',
                    'criteria']
