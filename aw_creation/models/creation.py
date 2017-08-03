import calendar
import json
import logging
import uuid
from decimal import Decimal
from PIL import Image
from django.core.validators import MaxValueValidator, MinValueValidator, \
    RegexValidator
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from datetime import datetime
import pytz
import re

logger = logging.getLogger(__name__)

VIDEO_AD_THUMBNAIL_SIZE = (300, 60)
BULK_CREATE_CAMPAIGNS_COUNT = 5
BULK_CREATE_AD_GROUPS_COUNT = 5

WEEKDAYS = list(calendar.day_name)
NameValidator = RegexValidator(r"^[^#']*$",
                               "# and ' are not allowed for titles")
YT_VIDEO_REGEX = r"^(?:https?:/{2})?(?:w{3}\.)?youtu(?:be)?\.(?:com|be)"\
                 r"(?:/watch\?v=|/video/)([^\s&]+)$"
VideoUrlValidator = RegexValidator(YT_VIDEO_REGEX, 'Wrong video url')
TrackingTemplateValidator = RegexValidator(
    r"(https?://\S+)|(\{lpurl\}\S*)",
    "Tracking url template must ba a valid URL or start with {lpurl} tag",
)


def get_uid(length=12):
    return str(uuid.uuid4()).replace('-', '')[:length]


def get_version():
    return get_uid(8)


def get_yt_id_from_url(url):
    match = re.match(YT_VIDEO_REGEX, url)
    if match:
        return match.group(1)


class UniqueItem(models.Model):

    name = models.CharField(max_length=250, validators=[NameValidator])

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

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

    account = models.ForeignKey(
        "aw_reporting.Account", related_name='account_creations',
        null=True, blank=True,
    )
    is_deleted = models.BooleanField(default=False)
    is_paused = models.BooleanField(default=False)
    is_ended = models.BooleanField(default=False)
    is_approved = models.BooleanField(default=False)
    is_managed = models.BooleanField(default=True)
    sync_at = models.DateTimeField(null=True)

    @property
    def is_changed(self):
        if self.sync_at and self.sync_at >= self.updated_at:
            return False
        return True

    @property
    def timezone(self):
        if self.account and self.account.timezone:
            return self.account.timezone
        else:
            from aw_reporting.models import DEFAULT_TIMEZONE
            return DEFAULT_TIMEZONE

    def get_today_date(self):
        return datetime.now(tz=pytz.timezone(self.timezone)).date()

    def get_aws_code(self, request):
        if self.account_id:
            lines = []
            for c in self.campaign_creations.filter(
                budget__isnull=False,
            ):
                lines.append(c.get_aws_code(request))
            lines.append(
                "sendChangesStatus('{}', '{}');".format(self.account_id, self.updated_at)
            )
            return "\n".join(lines)


@receiver(post_save, sender=AccountCreation, dispatch_uid="save_account_receiver")
def save_account_receiver(sender, instance, created, **_):
    if instance.is_deleted and not created:
        instance.is_deleted = False
        instance.save()


def default_languages():
    return Language.objects.filter(pk__in=(1000, 1003))


class Language(models.Model):
    name = models.CharField(max_length=50)
    code = models.CharField(max_length=5)


class CommonTargetingItem(UniqueItem):
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

    class Meta:
        abstract = True


class CampaignCreation(CommonTargetingItem):

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

    languages = models.ManyToManyField(
        'Language', related_name='campaigns', default=default_languages)

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

    def get_video_networks(self):
        return json.loads(self.video_networks_raw)

    def set_video_networks(self, value):
        self.video_networks_raw = json.dumps(value)
    video_networks = property(get_video_networks, set_video_networks)

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

    # content exclusions
    VIDEO_RATING_DV_MA_CONTENT_LABEL = "VIDEO_RATING_DV_MA"
    VIDEO_NOT_YET_RATED_CONTENT_LABEL = "VIDEO_NOT_YET_RATED"
    CONTENT_LABELS = (
        # ("ADULTISH", "Sexually suggestive content"),
        # ("AFE", "Error pages"),
        # ("BELOW_THE_FOLD", "Below the fold placements"),
        # ("CONFLICT", "Military & international conflict"),
        # ("DP", "Parked domains"),
        ("EMBEDDED_VIDEO", "Embedded video"),
        ("GAMES", "Games"),
        # ("JUVENILE", "Juvenile, gross & bizarre content"),
        # ("PROFANITY", "Profanity & rough language"),

        # ("UGC_FORUMS", "Forums"),
        # ("UGC_IMAGES", "Image-sharing pages"),
        # ("UGC_SOCIAL", "Social networks"),
        # ("UGC_VIDEOS", "Video-sharing pages"),

        # ("SIRENS", "Crime, police & emergency"),
        # ("TRAGEDY", "Death & tragedy"),
        # ("VIDEO", "Video"),
        ("VIDEO_RATING_DV_G", "Content rating: G"),
        ("VIDEO_RATING_DV_PG", "Content rating: PG"),
        ("VIDEO_RATING_DV_T", "Content rating: T"),
        (VIDEO_RATING_DV_MA_CONTENT_LABEL, "Content rating: MA"),
        (VIDEO_NOT_YET_RATED_CONTENT_LABEL, "Content rating: not yet rated"),
        ("LIVE_STREAMING_VIDEO", "Live streaming video"),
        # ("ALLOWED_GAMBLING_CONTENT", "Allowed gambling content"),
    )
    content_exclusions_raw = models.CharField(
        max_length=200,
        default=json.dumps(
            [VIDEO_RATING_DV_MA_CONTENT_LABEL, VIDEO_NOT_YET_RATED_CONTENT_LABEL]
        ),
    )

    def get_content_exclusions(self):
        return json.loads(self.content_exclusions_raw)

    def set_content_exclusions(self, value):
        self.content_exclusions_raw = json.dumps(value)

    content_exclusions = property(get_content_exclusions, set_content_exclusions)

    class Meta:
        ordering = ['-id']

    @property
    def campaign_is_paused(self):
        ac = self.account_creation
        return ac.is_paused or ac.is_ended or ac.is_deleted

    @property
    def start_for_creation(self):
        if self.start:
            return self.start
        elif self.account_creation.account:
            timezone = self.account_creation.timezone
            today = datetime.now(tz=pytz.timezone(timezone))
            return today

    def get_aws_code(self, request):

        lines = [
            "var campaign = createOrUpdateCampaign({});".format(
                json.dumps(dict(
                    id=self.id,
                    name=self.unique_name,
                    budget=str(self.budget),
                    start_for_creation=self.start_for_creation.strftime("%Y-%m-%d"),
                    budget_type="cpm" if self.video_ad_format == CampaignCreation.BUMPER_AD else "cpv",
                    is_paused='true' if self.campaign_is_paused else 'false',
                    start=self.start.strftime("%Y%m%d") if self.start else None,
                    end=self.end.strftime("%Y%m%d") if self.end else None,
                    video_networks=self.video_networks,
                    lang_ids=list(self.languages.values_list('id', flat=True)),
                    devices=self.devices,
                    schedules=[
                        "{} {} {} {} {}".format(
                            WEEKDAYS[s.day - 1].upper(), s.from_hour,
                            s.from_minute, s.to_hour, s.to_minute
                        ) for s in self.ad_schedule_rules.all()
                    ],
                    freq_caps={
                        f["event_type"]: f
                        for f in self.frequency_capping.all(
                        ).values("event_type", "level", "time_unit", "limit")
                    },
                    locations=list(
                        self.location_rules.filter(
                            geo_target_id__isnull=False
                        ).values_list("geo_target_id", flat=True)
                    ),
                    proximities=[
                        " ".join(
                            ("{}".format(l.latitude).rstrip('0'),
                             "{}".format(l.longitude).rstrip('0'),
                             str(l.radius), l.radius_units)
                        ) for l in self.location_rules.filter(radius__gte=0,
                                                              latitude__isnull=False,
                                                              longitude__isnull=False)
                    ],
                    content_exclusions=self.content_exclusions,
                ))
            )
        ]
        for ag in self.ad_group_creations.filter(max_rate__isnull=False):
            code = ag.get_aws_code(request)
            if code:
                lines.append(code)
        return "\n".join(lines)


@receiver(post_save, sender=CampaignCreation,
          dispatch_uid="save_campaign_receiver")
def save_campaign_receiver(sender, instance, created, **_):
    account_creation = instance.account_creation
    account_creation.is_approved = False
    account_creation.is_deleted = False
    account_creation.save()


class AdGroupCreation(CommonTargetingItem):

    max_rate = models.DecimalField(max_digits=6, decimal_places=3, null=True, blank=True)
    campaign_creation = models.ForeignKey(
        CampaignCreation, related_name="ad_group_creations",
    )
    ad_group = models.OneToOneField(
        "aw_reporting.AdGroup", related_name='ad_group_creation',
        on_delete=models.SET_NULL, null=True, blank=True,
    )

    class Meta:
        ordering = ['-id']

    def get_aws_code(self, request):
        """
        "campaign" variable have to be defined above
        :return:
        """
        from .targeting import TargetingItem
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
            return list(values)

        campaign = self.campaign_creation
        params = dict(
            id=self.id,
            name=self.unique_name,
            ad_format="VIDEO_{}".format(campaign.video_ad_format),
            cpv=str(self.max_rate),
            genders=self.genders or campaign.genders,
            parents=self.parents or campaign.parents,
            ages=self.age_ranges or campaign.age_ranges,
            channels=qs_to_list(channels.filter(is_negative=False)),
            channels_negative=qs_to_list(channels.filter(is_negative=True)),
            videos=qs_to_list(videos.filter(is_negative=False)),
            videos_negative=qs_to_list(videos.filter(is_negative=True)),
            topics=qs_to_list(topics.filter(is_negative=False), to_int=True),
            topics_negative=qs_to_list(topics.filter(is_negative=True), to_int=True),
            interests=qs_to_list(interests.filter(is_negative=False), to_int=True),
            interests_negative=qs_to_list(interests.filter(is_negative=True), to_int=True),
            keywords=qs_to_list(keywords.filter(is_negative=False)),
            keywords_negative=qs_to_list(keywords.filter(is_negative=True)),
        )
        lines = [
            "var ad_group = createOrUpdateAdGroup(campaign, {});".format(
               json.dumps(params)
            ),
        ]
        for ad in self.ad_creations.exclude(
            models.Q(video_url="") | models.Q(display_url="") | models.Q(display_url__isnull=True) |
            models.Q(final_url="") | models.Q(final_url__isnull=True)
        ):
            ad_params = dict(
                id=ad.id,
                name=ad.unique_name,
                video_url=ad.video_url,
                video_thumbnail=request.build_absolute_uri(ad.companion_banner.url)
                if ad.companion_banner else None,
                display_url=ad.display_url,
                final_url=ad.final_url,
                tracking_template=ad.tracking_template,
                custom_params={p['name']: p['value'] for p in ad.custom_params},
            )
            lines.append(
                "createOrUpdateVideoAd(ad_group, {});".format(
                    json.dumps(ad_params)
                )
            )

        return "\n".join(lines)

# these targeting fields can be empty at the ad_group level,
# because they are defined at the campaign level
AdGroupCreation._meta.get_field('genders_raw').default = json.dumps([])
AdGroupCreation._meta.get_field('parents_raw').default = json.dumps([])
AdGroupCreation._meta.get_field('age_ranges_raw').default = json.dumps([])


@receiver(post_save, sender=AdGroupCreation,
          dispatch_uid="save_group_receiver")
def save_group_receiver(sender, instance, created, **_):
    account_creation = AccountCreation.objects.get(campaign_creations__ad_group_creations=instance)
    account_creation.is_approved = False
    account_creation.is_deleted = False
    account_creation.save()


class AdCreation(UniqueItem):
    ad_group_creation = models.ForeignKey(
        AdGroupCreation, related_name="ad_creations",
    )
    video_url = models.URLField(validators=[VideoUrlValidator], default="")
    companion_banner = models.ImageField(upload_to='img/custom_video_thumbs', blank=True, null=True)
    display_url = models.CharField(max_length=200, default="")
    final_url = models.URLField(default="")
    tracking_template = models.CharField(max_length=250, validators=[TrackingTemplateValidator], default="")

    # video details
    video_id = models.CharField(max_length=20, default="")
    video_title = models.CharField(max_length=250, default="")
    video_description = models.TextField(default="")
    video_thumbnail = models.URLField(default="")
    video_channel_title = models.CharField(max_length=250, default="")

    def get_custom_params(self):
        return json.loads(self.custom_params_raw)

    def set_custom_params(self, value):
        self.custom_params_raw = json.dumps(value)

    custom_params_raw = models.CharField(max_length=250, default=json.dumps([]))
    custom_params = property(get_custom_params, set_custom_params)

    class Meta:
        ordering = ['-id']

    def save(self, force_insert=False, force_update=False, using=None, update_fields=None):
        super(AdCreation, self).save(force_insert, force_update, using, update_fields)

        if self.companion_banner:
            image = Image.open(self.companion_banner)
            if VIDEO_AD_THUMBNAIL_SIZE != image.size:
                new_width = VIDEO_AD_THUMBNAIL_SIZE[0]
                percent = new_width / image.size[0]
                new_height = int(image.size[1] * percent)
                if new_height < VIDEO_AD_THUMBNAIL_SIZE[1]:  # a wide image
                    new_height = VIDEO_AD_THUMBNAIL_SIZE[1]
                    percent = new_height / image.size[1]
                    new_width = int(image.size[0] * percent)

                image = image.resize((new_width, new_height), Image.ANTIALIAS)
                image = image.crop((0, 0, VIDEO_AD_THUMBNAIL_SIZE[0], VIDEO_AD_THUMBNAIL_SIZE[1]))
                image.save(self.companion_banner.path)


@receiver(post_save, sender=AdCreation,
          dispatch_uid="save_group_receiver")
def save_ad_receiver(sender, instance, created, **_):
    account_creation = AccountCreation.objects.get(campaign_creations__ad_group_creations__ad_creations=instance)
    account_creation.is_approved = False
    account_creation.is_deleted = False
    account_creation.save()


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
    limit = models.PositiveIntegerField(
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