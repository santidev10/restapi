from rest_framework.serializers import ModelSerializer

from brand_safety.models import BadVideo


class BadVideoSerializer(ModelSerializer):
    class Meta:
        model = BadVideo
        fields = (
            "category",
            "id",
            "reason",
            "thumbnail_url",
            "title",
            "youtube_id",
        )
