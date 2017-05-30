from django.db import models
import re

SUM_STATS = ("impressions", "video_views", "clicks", "cost")
CONVERSIONS = ("all_conversions", "conversions", "view_through")

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


def get_average_cpv(cost, views):
    if cost is None or not views:
        return None
    return float(cost)/views


def get_average_cpm(cost, impressions):
    if cost is None or not impressions:
        return None
    return float(cost)/impressions*1000


CALCULATED_STATS = {
    'video_view_rate': {
        'dependencies': ('video_views', 'impressions'),
        'receipt': lambda views, impressions: 100 * views/float(impressions)
                                              if impressions else None
    },
    'ctr': {
        'dependencies': ('clicks', 'impressions'),
        'receipt': lambda clicks, impressions: 100 * clicks/float(impressions)
                                               if impressions else None
    },
    'ctr_v': {
        'dependencies': ('clicks', 'video_views'),
        'receipt': lambda clicks, views: 100 * clicks/float(views)
                                         if views else None
    },
    'average_cpv': {
        'dependencies': ('cost', 'video_views'),
        'receipt': get_average_cpv
    },
    'average_cpm': {
        'dependencies': ('cost', 'impressions'),
        'receipt': get_average_cpm
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


class AWConnection(models.Model):
    email = models.EmailField(primary_key=True)
    refresh_token = models.CharField(max_length=150)
    users = models.ManyToManyField('userprofile.userprofile',
                                   related_name="aw_connections")
    # Token has been expired or revoked
    revoked_access = models.BooleanField(default=False)

    def __str__(self):
        return "AWConnection: {}".format(self.email)


class Account(models.Model):
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250, null=True)
    currency_code = models.CharField(max_length=5, null=True)
    timezone = models.CharField(max_length=100, null=True)
    can_manage_clients = models.BooleanField(default=False)
    is_test_account = models.BooleanField(default=False)
    managers = models.ManyToManyField("self", related_name='customers')
    visible = models.BooleanField(default=True)

    def __str__(self):
        return "Account: {}".format(self.name)


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


class BaseStatisticModel(models.Model):
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
    status = models.CharField(max_length=7, null=True)

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


class Audience(models.Model):
    parent = models.ForeignKey('self', null=True, related_name='children')
    name = models.CharField(max_length=150)
    type = models.CharField(max_length=25, db_index=True)

    CUSTOM_AFFINITY_TYPE = "custom-affinity"
    AFFINITY_TYPE = "affinity"
    IN_MARKET_TYPE = "in-market"
    TYPES = (CUSTOM_AFFINITY_TYPE, AFFINITY_TYPE, IN_MARKET_TYPE)

    def __str__(self):
        return "%s" % self.name


class RemarkList(models.Model):
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250)

    def __str__(self):
        return self.name
