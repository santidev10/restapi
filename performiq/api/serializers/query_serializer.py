from rest_framework import serializers

from segment.api.serializers.ctl_params_serializer import AdsPerformanceRangeField
from segment.api.serializers.ctl_params_serializer import NullableListField


class IQCampaignQuerySerializer(serializers.Serializer):
    """
    Serializer dedicated to formatting field values for SegmentQueryBuilder
    """
    active_view_viewability = AdsPerformanceRangeField(default=None)
    average_cpm = AdsPerformanceRangeField(reverse=True, default=None)
    average_cpv = AdsPerformanceRangeField(reverse=True, default=None)
    content_categories = NullableListField(default=None)
    content_quality = serializers.ListField()
    content_type = serializers.ListField()
    ctr = AdsPerformanceRangeField(required=False, default=None)
    exclude_content_categories = NullableListField(default=None)
    score_threshold = serializers.IntegerField()
    video_quartile_100_rate = AdsPerformanceRangeField(default=None)
    video_view_rate = AdsPerformanceRangeField(default=None)
