from rest_framework.serializers import Serializer
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField

from video.api.views.video_export import YTVideoLinkFromID


class PersistentSegmentVideoExportSerializer(Serializer):
    # url = YTVideoLinkFromID(source="main.id")
    title = CharField(source="general_data.title")
    category = CharField(source="general_data.category")
    likes = IntegerField(source="stats.likes")
    dislikes = IntegerField(source="stats.dislikes")
    views = IntegerField(source="stats.views")
    overall_score = IntegerField(source="brand_safety.overall_score")
