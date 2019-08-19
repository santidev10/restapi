"""
Segment api serializers module
"""
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField


class PersistentSegmentVideoExportSerializer(Serializer):
    # Fields map to segment export rows
    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default=None)
    Language = CharField(source="brand_safety.language", default=None)
    Category = CharField(source="general_data.category", default=None)
    Likes = IntegerField(source="stats.likes", default=None)
    Dislikes = IntegerField(source="stats.dislikes", default=None)
    Views = IntegerField(source="stats.views", default=None)
    Overall_Score = IntegerField(source="brand_safety.overall_score", default=None)

    def get_url(self, obj):
        return f"https://www.youtube.com/video/{obj.main.id}/"


class PersistentSegmentChannelExportSerializer(Serializer):
    # Fields map to segment export rows
    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default=None)
    Language = CharField(source="brand_safety.language", default=None)
    Category = CharField(source="general_data.top_category", default=None)
    Subscribers = IntegerField(source="stats.subscribers", default=None)
    Likes = IntegerField(source="stats.observed_videos_likes", default=None)
    Dislikes = IntegerField(source="stats.observed_videos_dislikes", default=None)
    Views = IntegerField(source="stats.views", default=None)
    Audited_Videos = IntegerField(source="brand_safety.videos_scored", default=None)
    Overall_Score = IntegerField(source="brand_safety.overall_score", default=None)

    def get_url(self, obj):
        return f"https://www.youtube.com/channel/{obj.main.id}/"
