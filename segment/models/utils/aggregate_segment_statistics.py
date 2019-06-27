"""
Segment models utils module
"""
from datetime import datetime
from datetime import timedelta

from django.db.models import F
from aw_reporting.adwords_api import load_web_app_settings
from aw_reporting.models import Account
from aw_reporting.models.ad_words.calculations import multiply_percent
from userprofile.models import UserProfile
from utils.datetime import now_in_default_tz

from aw_reporting.models import get_average_cpv, get_average_cpm, get_video_view_rate, get_ctr, get_ctr_v


def aggregate_segment_statistics(segment):
    """
    Prepare adwords statistics for segment
    """
    user = segment.owner
    # prepare queryset
    mcc_acc, is_chf = get_mcc_to_update(user)
    related_ids = segment.related_ids
    filters = {
        "ad_group__campaign__account__managers": mcc_acc,
        "yt_id__in": related_ids,
        "date__gte": datetime.today() - timedelta(days=180)
    }
    aggregated = {
        "cost": 0,
        "video_views": 0,
        "clicks": 0,
        "impressions": 0,
        "video_clicks": 0,
        "video_impressions": 0,
    }
    queryset = segment.related_aw_statistics_model.objects.filter(**filters)\
        .select_related("ad_group") \
        .annotate(video_clicks=F("ad_group__video_views"), video_impressions=F("ad_group__video_views")) \
        .values("cost", "video_views", "clicks", "impressions", "video_clicks", "video_impressions")
    for statistic in queryset:
        for field, value in statistic.items():
            if field == "video_clicks" or field == "video_impressions" and value >= 0:
                aggregated[field] += value
            else:
                aggregated[field] += value
    result = {
        "stats": {
            "average_cpm": get_average_cpm(aggregated["cost"], aggregated["impressions"]),
            "average_cpv": get_average_cpv(aggregated["cost"], aggregated["video_views"]),
            "ctr_v": multiply_percent(get_ctr_v(aggregated["clicks"], aggregated["video_views"])),
            "ctr": multiply_percent(get_ctr(aggregated["clicks"], aggregated["impressions"])),
            "video_view_rate": multiply_percent(get_video_view_rate(aggregated["video_views"], aggregated["video_impressions"]))
        },
        "meta": {
            "account_id": mcc_acc.id,
            "account_name": mcc_acc.name,
            "updated_at": str(now_in_default_tz()),
            "is_chf": is_chf,
        }
    }
    return result

def get_mcc_to_update(user: UserProfile):
    return Account.objects.get(id=load_web_app_settings()["cf_account_id"]), True
