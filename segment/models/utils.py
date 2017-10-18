"""
Segment models utils module
"""
from django.db.models import Sum, Case, When, IntegerField

from aw_reporting.models import YTVideoStatistic, YTChannelStatistic,\
    Account, dict_add_calculated_stats
from segment.models import SegmentRelatedVideo, \
    SegmentRelatedChannel


def count_segment_adwords_statistics(segment, **kwargs):
    """
    Prepare adwords statistics for segment
    """
    # obtain user from kwargs -> serializer context -> request
    try:
        user = kwargs.get("request").user
    except AttributeError:
        raise AttributeError(
            "Serializer context with request is required in kwargs")
    # define appropriate models
    if segment.segment_type == "video":
        related_objects_model = SegmentRelatedVideo
        adwords_statistics_model = YTVideoStatistic
    else:
        related_objects_model = SegmentRelatedChannel
        adwords_statistics_model = YTChannelStatistic
    # obtain related to segment videos ids
    videos_ids = related_objects_model.objects.filter(
        segment_id=segment.id).values_list("related_id", flat=True)
    # obtain aw account
    accounts = Account.user_objects(user)
    # prepare aggregated statistics
    aggregated_data = adwords_statistics_model.objects.filter(
        ad_group__campaign__account__in=accounts,
        yt_id__in=videos_ids).aggregate(
        cost=Sum("cost"), video_views=Sum("video_views"),
        clicks=Sum("clicks"), impressions=Sum("impressions"),
        video_impressions=Sum(Case(When(
            ad_group__video_views__gt=0,
            then="impressions",
        ), output_field=IntegerField())))
    # count and add statistics fields
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
