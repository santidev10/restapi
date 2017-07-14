from django.db import models
from django.db.models.aggregates import Aggregate
from django.db.models import Min, Sum, Case, When, Value, F, IntegerField, FloatField
from keyword_tool.models import BaseModel
import re

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

DEFAULT_TIMEZONE = 'America/Los_Angeles'


def get_average_cpv(*args, **kwargs):
    if len(args) == 2:
        cost, video_views = args
    else:
        cost = kwargs['cost']
        video_views = kwargs['video_views']

    if video_views:
        return cost / video_views


def get_average_cpm(*args, **kwargs):
    if len(args) == 2:
        cost, impressions = args
    else:
        cost = kwargs['cost']
        impressions = kwargs['impressions']

    if cost is None or not impressions:
        return None
    return cost / impressions * 1000


def get_video_view_rate(*args, **kwargs):
    if len(args) == 2:
        views, impressions = args
    else:
        views = kwargs['video_views']
        impressions = kwargs['video_impressions']

    if impressions:
        return 100 * views / impressions


def get_ctr(*args, **kwargs):
    if len(args) == 2:
        clicks, impressions = args
    else:
        clicks = kwargs['clicks']
        impressions = kwargs['impressions']

    if impressions:
        return 100 * clicks / impressions


def get_ctr_v(*args, **kwargs):
    if len(args) == 2:
        clicks, video_views = args
    else:
        clicks = kwargs['clicks']
        video_views = kwargs['video_views']

    if video_views:
        return 100 * clicks / video_views


CALCULATED_STATS = {
    'video_view_rate': {
        'dependencies': ('video_views', 'impressions'),
        'receipt': get_video_view_rate,
    },
    'ctr': {
        'dependencies': ('clicks', 'impressions'),
        'receipt': get_ctr,
    },
    'ctr_v': {
        'dependencies': ('clicks', 'video_views'),
        'receipt': get_ctr_v,
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

base_stats_aggregate = dict(
    sum_impressions=Sum("impressions"),
    video_impressions=Sum(
        Case(
            When(
                video_views__gt=0,
                then="impressions",
            ),
            output_field=IntegerField()
        )
    ),
    sum_video_views=Sum("video_views"),
    sum_clicks=Sum("clicks"),
    sum_cost=Sum("cost"),
)

all_stats_aggregate = {"sum_{}".format(s): Sum(s) for s in QUARTILE_STATS + CONVERSIONS}
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
            value = ", ".join(str(i) for i in value)
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
    user = models.ForeignKey("userprofile.userprofile", related_name="aw_connections")
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
    managers = models.ManyToManyField("self", related_name='customers')
    visible = models.BooleanField(default=True)
    update_time = models.DateTimeField(null=True)

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


class Campaign(BaseStatisticModel):
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250)
    account = models.ForeignKey(Account, related_name='campaigns')

    start_date = models.DateField(null=True, db_index=True)
    end_date = models.DateField(null=True)
    type = models.CharField(max_length=20, null=True)
    budget = models.FloatField(null=True)
    status = models.CharField(max_length=10, null=True)
    update_time = models.DateTimeField(auto_now_add=True)

    SERVING_STATUSES = ("eligible", "pending", "suspended", "ended", "none")

    def __str__(self):
        return "%s" % self.name


class AdGroup(BaseStatisticModel):
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250)
    status = models.CharField(max_length=7, null=True)
    campaign = models.ForeignKey(Campaign)
    cpv_bid = models.PositiveIntegerField(null=True)
    cpm_bid = models.PositiveIntegerField(null=True)
    cpc_bid = models.PositiveIntegerField(null=True)

    def __str__(self):
        return "%s %s" % (self.campaign.name, self.name)


class Ad(BaseStatisticModel):
    id = models.CharField(max_length=15, primary_key=True)
    ad_group = models.ForeignKey(AdGroup, related_name='ads')

    headline = models.CharField(max_length=150, null=True)
    creative_name = models.CharField(max_length=150, null=True)
    display_url = models.CharField(max_length=150, null=True)
    status = models.CharField(max_length=10, null=True)

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
    status = models.CharField(max_length=10)

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
