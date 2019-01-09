from aw_creation.api.serializers.analytics.base_account_creation_serializer import BaseAccountCreationSerializer
from aw_reporting.models import base_stats_aggregator


class AnalyticsAccountCreationListSerializer(BaseAccountCreationSerializer):
    stats_aggregations = base_stats_aggregator()

    class Meta(BaseAccountCreationSerializer.Meta):
        fields = (
            "account",
            "ad_count",
            "average_cpm",
            "average_cpv",
            "channel_count",
            "clicks",
            "cost",
            "ctr",
            "ctr_v",
            "details",
            "end",
            "from_aw",
            "id",
            "impressions",
            "interest_count",
            "is_changed",
            "is_disapproved",
            "is_editable",
            "is_managed",
            "keyword_count",
            "name",
            "plan_cpm",
            "plan_cpv",
            "start",
            "status",
            "thumbnail",
            "topic_count",
            "updated_at",
            "video_count",
            "video_view_rate",
            "video_views",
            "weekly_chart",
        )
