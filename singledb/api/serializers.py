from rest_framework.serializers import ModelSerializer

from singledb.models import Channel
from singledb.models import Video


class ChannelSerializer(ModelSerializer):
    class Meta:
        model = Channel
        fields = (
            "id",
            "title",
            "description",
            "tags",
            "youtube_keywords",
            "emails",
            "thumbnail_image_url",
            "youtube_published_at",
            "updated_at",
            "country",
            "category",
            "social_stats",
            "social_links",
            "is_monetizable",
            "monetization",
            "content_owner",
            "details",
            "from_ad_words",
            "preferred",
            "ptk_value",
            "is_content_safe",
            "subscribers",
            "thirty_days_subscribers",
            "thirty_days_views",
            "views_per_video",
            "sentiment",
            "engage_rate",
            "social_stats",
        )


class VideoChannelSerializer(ModelSerializer):
    class Meta:
        model = Channel
        fields = (
            "id",
            "title",
            "description",
            "tags",
            "youtube_keywords",
            "emails",
            "thumbnail_image_url",
            "youtube_published_at",
            "updated_at",
            "country",
            "category",
            "social_stats",
            "social_links",
            "is_monetizable",
            "monetization",
            "content_owner",
            "details",
            "from_ad_words",
            "preferred",
            "ptk_value",
            "is_content_safe",
            "subscribers",
            "thirty_days_subscribers",
            "thirty_days_views",
            "views_per_video",
            "sentiment",
            "engage_rate",
            "social_stats",
        )


class VideoSerializer(ModelSerializer):
    channel = VideoChannelSerializer()

    class Meta:
        model = Video
        fields = (
            "id",
            "title",
            "channel",
            "views",
            "likes",
            "dislikes",
            "comments",
            "sentiment",
            "engage_rate",
            "youtube_published_at",
            "updated_at",
            "thumbnail_image_url",
            "description",
            "country",
            "category",
            "is_monetizable",
            "consideration",
            "tags",
            "details",
            "ptk_value",
            "from_ad_words",
            "is_content_safe",
            "is_ptk_customized"
        )


