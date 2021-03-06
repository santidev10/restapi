"""
Segment models utils module
"""
from django.db.models import Case
from django.db.models import IntegerField
from django.db.models import Sum
from django.db.models import When

from aw_reporting.adwords_api import load_web_app_settings
from aw_reporting.models import Account
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from userprofile.models import UserProfile
from utils.datetime import now_in_default_tz


def count_segment_adwords_statistics(segment):
    """
    Prepare adwords statistics for segment
    """
    # obtain user from segment owner field
    try:
        user = segment.owner
    # pylint: disable=broad-except
    except Exception:
        # pylint: enable=broad-except
        return {}

    # obtain related to segments related ids
    related_ids = segment.related.model.objects \
        .filter(segment_id=segment.id) \
        .values_list("related_id", flat=True)
    # prepare queryset

    mcc_acc, is_chf = get_mcc_to_update(user)
    filters = {
        "ad_group__campaign__account__managers": mcc_acc,
    }

    if segment.segment_type == "keyword":
        filters["keyword__in"] = related_ids
    else:
        filters["yt_id__in"] = related_ids

    queryset = segment.config.RELATED_STATISTICS_MODEL.objects.filter(**filters)
    # prepare aggregated statistics
    aggregated_data = queryset.aggregate(
        sum_cost=Sum("cost"), sum_video_views=Sum("video_views"),
        sum_clicks=Sum("clicks"), sum_impressions=Sum("impressions"),
        sum_video_clicks=Sum(Case(When(
            ad_group__video_views__gt=0,
            then="clicks",
        ), output_field=IntegerField())),
        sum_video_impressions=Sum(Case(When(
            ad_group__video_views__gt=0,
            then="impressions",
        ), output_field=IntegerField())))
    # count and add statistics fields
    dict_norm_base_stats(aggregated_data)
    dict_add_calculated_stats(aggregated_data)
    # clean up
    fields_to_clean_up = [
        "cost",
        "video_views",
        "clicks",
        "impressions",
        "video_impressions",
        "video_clicks",
        "average_cpm"
    ]
    aggregated_data = {key: value
                       for key, value in aggregated_data.items()
                       if key not in fields_to_clean_up}

    return dict(
        stats=aggregated_data,
        meta=dict(
            account_id=mcc_acc.id,
            account_name=mcc_acc.name,
            updated_at=str(now_in_default_tz()),
            is_chf=is_chf
        )
    )


def get_mcc_to_update(user: UserProfile):
    return Account.objects.get(id=load_web_app_settings()["cf_account_id"]), True
