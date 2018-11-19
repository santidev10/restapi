from functools import wraps

from django.db.models import Case
from django.db.models import F
from django.db.models import IntegerField
from django.db.models import Sum
from django.db.models import When

from aw_reporting.models.ad_words.constants import CONVERSIONS
from aw_reporting.models.ad_words.constants import QUARTILE_STATS


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
    "video_view_rate": {
        "args": ("video_views", "video_impressions"),
        "receipt": multiply_percent(get_video_view_rate),
    },
    "ctr": {
        "args": ("clicks", "impressions"),
        "receipt": multiply_percent(get_ctr),
    },
    "ctr_v": {
        "args": ("video_clicks", "video_views"),
        "receipt": multiply_percent(get_ctr_v),
    },
    "average_cpv": {
        "args": ("cost", "video_views"),
        "receipt": get_average_cpv,
    },
    "average_cpm": {
        "args": ("cost", "impressions"),
        "receipt": get_average_cpm,
    },
}


def dict_add_calculated_stats(data):
    for n, i in CALCULATED_STATS.items():
        args_names = i.get('args', tuple())
        args = [data.get(d) for d in args_names]

        kwargs_names = i.get('kwargs', tuple())
        kwargs_map = i.get('kwargs_map', tuple())
        rec = i['receipt']

        kwargs = dict(
            tuple((
                (key, data.get(key)) for key in kwargs_names
            ))
            + tuple((
                (fn_key, data.get(data_key))
                for data_key, fn_key in kwargs_map
            ))
        )
        data[n] = None if None in args else rec(*args, **kwargs)


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
        sum_cost=Sum("cost"),
    )


def aw_placement_annotation(*keys, prefix=""):
    return dict((
        (key, F(prefix + "salesforce_placement__" + key)) for key in keys
    ))


CLIENT_COST_REQUIRED_FIELDS = ("goal_type_id", "total_cost", "ordered_rate",
                               "dynamic_placement", "placement_type",
                               "tech_fee")

client_cost_campaign_required_annotation = aw_placement_annotation(
    *CLIENT_COST_REQUIRED_FIELDS, prefix=""
)

client_cost_ad_group_statistic_required_annotation = aw_placement_annotation(
    *CLIENT_COST_REQUIRED_FIELDS, prefix="ad_group__campaign__"
)


def all_stats_aggregator(prefix=None):
    res = {"sum_{}".format(s): Sum(s)
           for s in QUARTILE_STATS + CONVERSIONS}
    res.update(base_stats_aggregator(prefix))
    return res


def dict_norm_base_stats(data):
    for k, v in list(data.items()):
        if k.startswith("sum_"):
            data[k[4:]] = v
            del data[k]
