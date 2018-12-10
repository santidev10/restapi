from rest_framework.serializers import ModelSerializer

from brand_safety.models import BadChannel


class BadChannelSerializer(ModelSerializer):
    class Meta:
        model = BadChannel
        fields = (
            "category",
            "id",
            "reason",
            "thumbnail_url",
            "title",
            "youtube_id",
        )
