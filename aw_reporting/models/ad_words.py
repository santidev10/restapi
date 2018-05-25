import re
from datetime import datetime
from functools import wraps

import pytz
from django.conf import settings
from django.db import models
from django.db.models import Min, Sum, Case, When, IntegerField, Q
from django.db.models.aggregates import Aggregate
from django.db.models.query import QuerySet as DBQuerySet

from aw_reporting.models.base import BaseModel
from aw_reporting.models.salesforce import OpPlacement
from aw_reporting.settings import InstanceSettings

BASE_STATS = ("impressions", "video_views", "clicks", "cost")
CONVERSIONS = ("all_conversions", "conversions", "view_through")

SUM_STATS = BASE_STATS + CONVERSIONS

QUARTILE_RATES = ('quartile_25_rate', 'quartile_50_rate',
                  'quartile_75_rate', 'quartile_100_rate')

QUARTILE_STATS = ("video_views_25_quartile", "video_views_50_quartile",
                  "video_views_75_quartile", "video_views_100_quartile")

VIEW_RATE_STATS = ("video25rate", "video50rate",
                   "video75rate", "video100rate")

Devices = ('Computers', 'Mobile devices with full browsers',
           'Tablets with full browsers', 'Other')

AgeRanges = ("Undetermined", "18-24", "25-34", "35-44", "45-54", "55-64",
             "65 or more",)

Genders = ("Undetermined", "Female", "Male")

DATE_FORMAT = "%Y-%m-%d"
ACTION_STATUSES = ("paused", "removed")

AgeRangeOptions = (
    "AGE_RANGE_UNDETERMINED",
    "AGE_RANGE_18_24",
    "AGE_RANGE_25_34",
    "AGE_RANGE_35_44",
    "AGE_RANGE_45_54",
    "AGE_RANGE_55_64",
    "AGE_RANGE_65_UP",
)

GenderOptions = (
    "GENDER_UNDETERMINED",
    "GENDER_FEMALE",
    "GENDER_MALE",
)


def get_average_cpv(*args, **kwargs):
    if len(args) == 2:
        cost, video_views = args
    elif 'cost' in kwargs and 'video_views' in kwargs:
        cost = kwargs['cost'] or 0
        video_views = kwargs['video_views']
    else:
        return

    if video_views:
        return cost / video_views


def get_average_cpm(*args, **kwargs):
    if len(args) == 2:
        cost, impressions = args
    elif 'cost' in kwargs and 'impressions' in kwargs:
        cost = kwargs['cost']
        impressions = kwargs['impressions']
    else:
        return

    if cost is None or not impressions:
        return None
    return cost / impressions * 1000


def get_video_view_rate(*args, **kwargs):
    if len(args) == 2:
        views, impressions = args
    elif 'video_impressions' in kwargs and 'video_views' in kwargs:
        views = kwargs['video_views']
        impressions = kwargs['video_impressions']
    else:
        return

    if impressions:
        return views / impressions


def get_ctr(*args, **kwargs):
    if len(args) == 2:
        clicks, impressions = args
    elif 'clicks' in kwargs and 'impressions' in kwargs:
        clicks = kwargs['clicks']
        impressions = kwargs['impressions']
    else:
        return

    if impressions:
        return clicks / impressions


def get_ctr_v(*args, **kwargs):
    if len(args) == 2:
        clicks, video_views = args
    elif "video_clicks" in kwargs and "video_views" in kwargs:
        clicks = kwargs["video_clicks"]
        video_views = kwargs["video_views"]
    else:
        return
    if video_views:
        return clicks / video_views


def get_margin(*_, plan_cost, cost, client_cost=None):
    """
    Margin calculation
    :param _:
    :param plan_cost: Budget IO from SF, maximum allowed cost
    :param cost: real cost from AdWords
    :param client_cost: shorter calculation with goal_type, plan_cost, cost and client_cost only
    :return:
    """
    # IF actual spend is below contracted, use actual. Else, use contracted budget (from SalesForce)
    if plan_cost is not None and client_cost > plan_cost:
        client_cost = plan_cost
    cost = cost or 0
    client_cost = client_cost or 0
    if client_cost == 0:
        return 0 if cost == 0 else -1
    return 1 - cost / client_cost


def multiply_percent(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        value = fn(*args, **kwargs)
        if isinstance(value, (int, float)):
            value *= 100
        return value
    return wrapper


CALCULATED_STATS = {
    'video_view_rate': {
        'dependencies': ('video_views', 'video_impressions'),
        'receipt': multiply_percent(get_video_view_rate),
    },
    'ctr': {
        'dependencies': ('clicks', 'impressions'),
        'receipt': multiply_percent(get_ctr),
    },
    'ctr_v': {
        'dependencies': ('clicks', 'video_views'),
        'receipt': multiply_percent(get_ctr_v),
    },
    'average_cpv': {
        'dependencies': ('cost', 'video_views'),
        'receipt': get_average_cpv,
    },
    'average_cpm': {
        'dependencies': ('cost', 'impressions'),
        'receipt': get_average_cpm,
    },
}


def dict_add_calculated_stats(data):
    for n, i in CALCULATED_STATS.items():
        dep = i['dependencies']
        rec = i['receipt']
        args = [data.get(d) for d in dep]
        data[n] = None if None in args else rec(*args)


def dict_quartiles_to_rates(data):
    impressions = data.get('impressions')
    for i in range(1, 5):
        qart = i * 25
        qf = "video_views_%d_quartile" % qart
        qv = data.get(qf)
        rf = "video%drate" % qart
        data[rf] = data[qf] / impressions * 100 \
            if impressions and qv is not None else None
        if qf in data:
            del data[qf]


def base_stats_aggregator(prefix=None):
    prefix = prefix or ""
    return dict(
        sum_impressions=Sum("impressions"),
        video_impressions=Sum(
            Case(
                When(**{
                    "{}video_views__gt".format(prefix): 0,
                    "then": "impressions",
                }),
                output_field=IntegerField()
            )
        ),
        video_clicks=Sum(
            Case(
                When(**{
                    "{}video_views__gt".format(prefix): 0,
                    "then": "clicks",
                }),
                output_field=IntegerField()
            )
        ),
        sum_video_views=Sum("video_views"),
        sum_clicks=Sum("clicks"),
        sum_cost=Sum("cost")
    )

# fixme: deprecated
base_stats_aggregate = base_stats_aggregator()

all_stats_aggregate = {"sum_{}".format(s): Sum(s)
                       for s in QUARTILE_STATS + CONVERSIONS}
all_stats_aggregate.update(base_stats_aggregate)


def dict_norm_base_stats(data):
    for k, v in list(data.items()):
        if k.startswith("sum_"):
            data[k[4:]] = v
            del data[k]


def dict_calculate_stats(data):
    for n, i in CALCULATED_STATS.items():
        rec = i['receipt']
        data[n] = rec(**data)


class ConcatAggregate(Aggregate):
    function = 'array_agg'
    name = 'Concat'
    template = '%(function)s(%(distinct)s%(expressions)s)'

    def __init__(self, expression, distinct=False, **extra):
        super(ConcatAggregate, self).__init__(
            expression,
            distinct='DISTINCT ' if distinct else '',
            **extra
        )

    def as_sqlite(self, compiler, connection, *args, **kwargs):
        return super(ConcatAggregate, self).as_sql(
            compiler, connection,
            template="GROUP_CONCAT(%(distinct)s%(expressions)s)",
            *args, **kwargs
        )

    def convert_value(self, value, expression, connection, context):
        if value is None:
            return ""
        if type(value) is str:
            value = value.split(",")
        if type(value) is list:
            value = ", ".join(str(i) for i in value if i is not None)
        return value


class AWConnection(models.Model):
    email = models.EmailField(primary_key=True)
    refresh_token = models.CharField(max_length=150)

    # Token has been expired or revoked
    revoked_access = models.BooleanField(default=False)

    def __str__(self):
        return "AWConnection: {}".format(self.email)


class AWConnectionToUserRelation(models.Model):
    connection = models.ForeignKey(AWConnection, related_name="user_relations")
    user = models.ForeignKey(settings.AUTH_USER_MODEL,
                             related_name="aw_connections")
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = (("user", "connection"),)


class Account(models.Model):
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250, null=True)
    currency_code = models.CharField(max_length=5, null=True)
    timezone = models.CharField(max_length=100, null=True)
    can_manage_clients = models.BooleanField(default=False)
    is_test_account = models.BooleanField(default=False)
    managers = models.ManyToManyField("self")
    visible = models.BooleanField(default=True)
    update_time = models.DateTimeField(null=True)
    hourly_updated_at = models.DateTimeField(null=True)
    settings_updated_at = models.DateTimeField(null=True)

    def __str__(self):
        return "Account: {}".format(self.name)

    @classmethod
    def user_mcc_objects(cls, user):
        qs = Account.objects.filter(
            mcc_permissions__aw_connection__user_relations__user=user
        ).order_by('id').distinct()
        return qs

    @classmethod
    def user_objects(cls, user):
        manager_ids = set(
            Account.objects.filter(
                mcc_permissions__aw_connection__user_relations__user=user
            ).values_list('id', flat=True)
        )
        qs = cls.objects.filter(
            managers__id__in=manager_ids,
        )
        return qs

    @property
    def start_date(self):
        return self.campaigns.aggregate(date=Min('start_date'))['date']

    @property
    def end_date(self):
        dates = self.campaigns.all().values_list('end_date', flat=True)
        if None not in dates and dates:
            return max(dates)


class AWAccountPermission(models.Model):
    aw_connection = models.ForeignKey(
        AWConnection, related_name="mcc_permissions")
    account = models.ForeignKey(
        Account, related_name="mcc_permissions")
    can_read = models.BooleanField(default=False)
    # we will check read permission every day and show data to those users
    # who has access to it on AdWords
    can_write = models.BooleanField(default=False)

    # we will be set True only after successful account creations
    # and set False on errors

    class Meta:
        unique_together = (("aw_connection", "account"),)

    def __str__(self):
        return "AWPermission({}, {})".format(self.aw_connection, self.account)


class BaseStatisticModel(BaseModel):
    impressions = models.IntegerField(default=0)
    video_views = models.IntegerField(default=0)
    clicks = models.IntegerField(default=0)
    cost = models.FloatField(default=0)
    conversions = models.FloatField(default=0)
    all_conversions = models.FloatField(default=0)
    view_through = models.IntegerField(default=0)
    video_views_25_quartile = models.FloatField(default=0)
    video_views_50_quartile = models.FloatField(default=0)
    video_views_75_quartile = models.FloatField(default=0)
    video_views_100_quartile = models.FloatField(default=0)

    class Meta:
        abstract = True

    def __getattr__(self, name):
        if name in CALCULATED_STATS:
            data = CALCULATED_STATS[name]
            dependencies = data['dependencies']
            receipt = data['receipt']
            return receipt(
                *[getattr(self, stat_name)
                  for stat_name in dependencies]
            )
        elif name in VIEW_RATE_STATS:
            quart = re.findall(r'\d+', name)[0]
            quart_views = getattr(self, 'video_views_%s_quartile' % quart)
            impressions = self.impressions
            return quart_views / impressions * 100 \
                if impressions else None
        else:
            raise AttributeError(self, name)


class ModelPlusDeNormFields(BaseStatisticModel):
    # for now we will use them in Pricing Tool
    de_norm_fields_are_recalculated = models.BooleanField(default=False)
    min_stat_date = models.DateField(null=True)
    max_stat_date = models.DateField(null=True)

    gender_undetermined = models.BooleanField(default=False)
    gender_male = models.BooleanField(default=False)
    gender_female = models.BooleanField(default=False)

    parent_parent = models.BooleanField(default=False)
    parent_not_parent = models.BooleanField(default=False)
    parent_undetermined = models.BooleanField(default=False)

    age_undetermined = models.BooleanField(default=False)
    age_18_24 = models.BooleanField(default=False)
    age_25_34 = models.BooleanField(default=False)
    age_35_44 = models.BooleanField(default=False)
    age_45_54 = models.BooleanField(default=False)
    age_55_64 = models.BooleanField(default=False)
    age_65 = models.BooleanField(default=False)

    device_computers = models.BooleanField(default=False)
    device_mobile = models.BooleanField(default=False)
    device_tablets = models.BooleanField(default=False)
    device_other = models.BooleanField(default=False)

    has_interests = models.BooleanField(default=False)
    has_keywords = models.BooleanField(default=False)
    has_channels = models.BooleanField(default=False)
    has_videos = models.BooleanField(default=False)
    has_remarketing = models.BooleanField(default=False)
    has_topics = models.BooleanField(default=False)

    class Meta:
        abstract = True


DEFAULT_TIMEZONE = settings.DEFAULT_TIMEZONE


class CampaignQueryset(DBQuerySet):
    def status(self, status, *args, tz=None, **kwargs):
        if status == 'paused':
            return self.filter(_status=status, *args, **kwargs)
        else:
            tz = tz or DEFAULT_TIMEZONE
            if type(tz) is str:
                tz = pytz.timezone(tz)
            today = datetime.now(tz=tz).date()
            if status == 'ended':
                return self.filter(
                    _status='enabled',
                    end_date__lt=today, *args, **kwargs
                )
            elif status == 'not_ended':
                return self.filter(
                    Q(end_date__gte=today) | Q(end_date__isnull=True),
                    *args, **kwargs
                )
            elif status == 'enabled':
                return self.filter(_status='enabled').filter(
                    Q(end_date__gte=today) | Q(end_date__isnull=True),
                    *args, **kwargs
                )
            else:
                raise ValueError("Unrecognized status '%s'" % status)


class CampaignQuerySetManager(models.Manager):

    def get_queryset(self):
        return CampaignQueryset(self.model, using=self._db)

    def visible_campaigns(self):
        visible_accounts = InstanceSettings().get("visible_accounts")
        return self.get_queryset().filter(Q(account__in=visible_accounts)
                                          | Q(account__isnull=True))


class Campaign(ModelPlusDeNormFields):
    objects = CampaignQuerySetManager()

    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250)
    account = models.ForeignKey(Account, null=True, related_name='campaigns')

    start_date = models.DateField(null=True, db_index=True)
    end_date = models.DateField(null=True)
    type = models.CharField(max_length=20, null=True)
    budget = models.FloatField(null=True)
    status = models.CharField(max_length=10, null=True)
    update_time = models.DateTimeField(auto_now_add=True)
    salesforce_placement = models.ForeignKey(
        OpPlacement,
        null=True,
        related_name='adwords_campaigns',
        on_delete=models.SET_NULL,
    )
    goal_allocation = models.FloatField(default=0)

    # setup fields
    targeting_interests = models.BooleanField(default=False)
    targeting_topics = models.BooleanField(default=False)
    targeting_keywords = models.BooleanField(default=False)
    targeting_channels = models.BooleanField(default=False)
    targeting_videos = models.BooleanField(default=False)
    targeting_remarketings = models.BooleanField(default=False)
    targeting_custom_affinity = models.BooleanField(default=False)
    tracking_template_is_set = models.BooleanField(default=False)
    targeting_excluded_channels = models.BooleanField(default=False)
    targeting_excluded_topics = models.BooleanField(default=False)
    targeting_excluded_keywords = models.BooleanField(default=False)

    _start = models.DateField(null=True)
    _end = models.DateField(null=True)
    placement_code = models.CharField(max_length=10, null=True, default=None)

    SERVING_STATUSES = ("eligible", "pending", "suspended", "ended", "none")

    @property
    def start(self):
        return self._start or self.start_date

    @property
    def end(self):
        return self._end or self.end_date

    def __str__(self):
        return "%s" % self.name


class AdGroup(ModelPlusDeNormFields):
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250)
    status = models.CharField(max_length=7, null=True)
    type = models.CharField(max_length=25, default="")
    campaign = models.ForeignKey(Campaign, related_name='ad_groups')
    engagements = models.IntegerField(default=0)
    active_view_impressions = models.IntegerField(default=0)
    cpv_bid = models.PositiveIntegerField(null=True)
    cpm_bid = models.PositiveIntegerField(null=True)
    cpc_bid = models.PositiveIntegerField(null=True)

    def __str__(self):
        return "%s %s" % (self.campaign.name, self.name)


class Ad(BaseStatisticModel):
    id = models.CharField(max_length=15, primary_key=True)
    ad_group = models.ForeignKey(AdGroup, related_name='ads')

    headline = models.TextField(null=True)
    creative_name = models.TextField(null=True)
    display_url = models.TextField(null=True)
    status = models.CharField(max_length=10, null=True)
    is_disapproved = models.BooleanField(default=False, null=False)

    def __str__(self):
        return "%s #%s" % (self.creative_name, self.id)


class VideoCreative(BaseStatisticModel):
    id = models.CharField(max_length=255, primary_key=True)
    duration = models.IntegerField(null=True)


class GeoTarget(models.Model):
    name = models.CharField(max_length=100)
    canonical_name = models.CharField(max_length=100)
    parent = models.ForeignKey('self', null=True)
    country_code = models.CharField(max_length=2)
    target_type = models.CharField(max_length=50)
    status = models.CharField(max_length=20)

    def __str__(self):
        return "%s" % self.canonical_name


class Topic(models.Model):
    parent = models.ForeignKey('self', null=True, related_name='children')
    name = models.CharField(max_length=150, db_index=True)

    def __str__(self):
        return self.name


class Audience(BaseModel):
    parent = models.ForeignKey('self', null=True, related_name='children')
    name = models.CharField(max_length=150)
    type = models.CharField(max_length=25, db_index=True)

    CUSTOM_AFFINITY_TYPE = "custom-affinity"
    AFFINITY_TYPE = "affinity"
    IN_MARKET_TYPE = "in-market"
    TYPES = (CUSTOM_AFFINITY_TYPE, AFFINITY_TYPE, IN_MARKET_TYPE)

    def __str__(self):
        return "%s" % self.name


class RemarkList(BaseModel):
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250)

    def __str__(self):
        return self.name


class CampaignAgeRangeTargeting(models.Model):
    age_range_id = models.SmallIntegerField()
    campaign = models.ForeignKey(Campaign, related_name="age_range_targeting")

    class Meta:
        unique_together = (("age_range_id", "campaign"),)


class CampaignGenderTargeting(models.Model):
    gender_id = models.SmallIntegerField()
    campaign = models.ForeignKey(Campaign, related_name="gender_targeting")

    class Meta:
        unique_together = (("gender_id", "campaign"),)


class CampaignLocationTargeting(models.Model):
    location = models.ForeignKey(GeoTarget)
    campaign = models.ForeignKey(Campaign, related_name="location_targeting")

    class Meta:
        unique_together = (("location", "campaign"),)
