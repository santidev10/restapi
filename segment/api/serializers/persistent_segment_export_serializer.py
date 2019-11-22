"""
Segment api serializers module
"""
from math import floor

from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from brand_safety.languages import LANGUAGES


class PersistentSegmentVideoExportSerializer(Serializer):
    columns = ("URL", "Title", "Language", "Category", "Likes", "Dislikes", "Views", "Overall_Score")

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default=None)
    Language = SerializerMethodField("get_language")
    Category = CharField(source="general_data.category", default=None)
    Likes = IntegerField(source="stats.likes", default=None)
    Dislikes = IntegerField(source="stats.dislikes", default=None)
    Views = IntegerField(source="stats.views", default=None)
    Overall_Score = SerializerMethodField("get_overall_score")

    def get_url(self, obj):
        return f"https://www.youtube.com/video/{obj.main.id}/"

    def get_language(self, obj):
        brand_safety_language = getattr(obj.brand_safety, "language", None)
        if brand_safety_language == "all":
            language = "All"
        else:
            language = LANGUAGES.get(brand_safety_language, brand_safety_language)
        return language

    def get_overall_score(self, obj):
        overall_score = obj.brand_safety.overall_score
        if overall_score:
            score = floor(obj.brand_safety.overall_score / 10)
        else:
            score = overall_score
        return score


class PersistentSegmentChannelExportSerializer(Serializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Likes", "Dislikes", "Views", "Audited_Videos", "Overall_Score")

    # Fields map to segment export rows
    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default=None)
    Language = SerializerMethodField("get_language")
    Category = CharField(source="general_data.top_category", default=None)
    Subscribers = IntegerField(source="stats.subscribers", default=None)
    Likes = IntegerField(source="stats.observed_videos_likes", default=None)
    Dislikes = IntegerField(source="stats.observed_videos_dislikes", default=None)
    Views = IntegerField(source="stats.views", default=None)
    Audited_Videos = IntegerField(source="brand_safety.videos_scored", default=None)
    Overall_Score = SerializerMethodField("get_overall_score")

    def get_url(self, obj):
        return f"https://www.youtube.com/channel/{obj.main.id}/"

    def get_language(self, obj):
        brand_safety_language = getattr(obj.brand_safety, "language", None)
        if brand_safety_language == "all":
            language = "All"
        else:
            language = LANGUAGES.get(brand_safety_language, brand_safety_language)
        return language

    def get_overall_score(self, obj):
        overall_score = obj.brand_safety.overall_score
        if overall_score:
            score = floor(obj.brand_safety.overall_score / 10)
        else:
            score = overall_score
        return score
