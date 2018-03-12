"""
Segment models utils module
"""
from django.db.models import Sum, Case, When, IntegerField

from aw_reporting.adwords_api import load_web_app_settings
from aw_reporting.models import Account, dict_add_calculated_stats, \
    dict_norm_base_stats


def count_segment_adwords_statistics(segment):
    """
    Prepare adwords statistics for segment
    """
    # obtain user from segment owner field
    try:
        user = segment.owner
    except Exception:
        return {}

    # obtain related to segments related ids
    related_ids = segment.related.model.objects.filter(segment_id=segment.id).values_list("related_id", flat=True)
    # obtain aw account
    accounts = Account.user_objects(user)
    # prepare queryset
    filters = {
        "ad_group__campaign__account__in": accounts,
    }

    if segment.segment_type == 'keyword':
        filters['keyword__in'] = related_ids
    else:
        filters['yt_id__in'] = related_ids

    queryset = segment.related_aw_statistics_model.objects.filter(**filters)
    # if no queryset is empty - show CHF data
    if not queryset.exists():
        accounts = Account.objects.filter(
            managers__id=load_web_app_settings()['cf_account_id'])
        filters['ad_group__campaign__account__in'] = accounts
        queryset = queryset.model.objects.filter(**filters)
    # prepare aggregated statistics
    aggregated_data = queryset.aggregate(
        sum_cost=Sum("cost"), sum_video_views=Sum("video_views"),
        sum_clicks=Sum("clicks"), sum_impressions=Sum("impressions"),
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
        "average_cpm"
    ]
    [aggregated_data.pop(key, None) for key in fields_to_clean_up]
    # finalize statistics data
    return aggregated_data
