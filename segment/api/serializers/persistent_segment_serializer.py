from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField

from segment.models.persistent.video import PersistentSegmentVideo


class PersistentSegmentSerializer(ModelSerializer):
    statistics = SerializerMethodField()

    statistics_fields = (
        "subscribers",
        "likes",
        "dislikes",
        "views",
        "audited_videos",
        "items_count",
        "average_cpm",
        "average_cpv",
        "ctr",
        "ctr_v",
        "video_view_rate",
        "average_brand_safety_score",
        "monthly_subscribers",
        "monthly_views",
    )

    class Meta:
        # fixme: replace PersistentSegmentVideo with None. It's a workaround to fix documentation generation
        model = PersistentSegmentVideo
        fields = (
            "id",
            "title",
            "segment_type",
            "statistics",
            "thumbnail_image_url",
            "created_at",
            "updated_at",
            "category",
            "is_master",
        )

    def get_statistics(self, obj):
        details = obj.details or {}
        statistics = {field: details[field] for field in self.statistics_fields if field in details.keys()}
        return statistics
