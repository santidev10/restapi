from django.db import models

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
