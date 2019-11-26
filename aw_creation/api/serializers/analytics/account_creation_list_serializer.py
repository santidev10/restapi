from aw_creation.api.serializers.analytics.base_account_creation_serializer import BaseAccountCreationSerializer
from aw_creation.api.serializers.common.stats_aggregator import stats_aggregator


class AnalyticsAccountCreationListSerializer(BaseAccountCreationSerializer):
    stats_aggregations = stats_aggregator(ad_group_stats_prefix="ad_groups__statistics__")

    class Meta(BaseAccountCreationSerializer.Meta):
        fields = (
            "account",
            "all_conversions",
            "average_cpm",
            "average_cpv",
            "clicks",
            "cost",
            "ctr",
            "ctr_v",
            "details",
            "end",
            "from_aw",
            "id",
            "impressions",
            "is_changed",
            "is_disapproved",
            "is_editable",
            "is_managed",
            "name",
            "plan_cpm",
            "plan_cpv",
            "start",
            "statistic_max_date",
            "statistic_min_date",
            "status",
            "thumbnail",
            "updated_at",
            "video_view_rate",
            "video_views",
            "weekly_chart",
        )
