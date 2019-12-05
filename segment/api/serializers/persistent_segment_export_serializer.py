"""
Segment api serializers module
"""
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from brand_safety.languages import LANGUAGES
from es_components.iab_categories import YOUTUBE_TO_IAB_CATEGORIES_MAPPING
from utils.brand_safety import map_brand_safety_score


class PersistentSegmentVideoExportSerializer(Serializer):
    columns = ("URL", "Title", "Language", "Category", "Likes", "Dislikes", "Views", "Overall_Score")

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default=None)
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
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
        score = map_brand_safety_score(obj.brand_safety.overall_score)
        return score

    def get_category(self, obj):
        youtube_category = getattr(obj.general_data, "category", "").lower()
        iab_category = YOUTUBE_TO_IAB_CATEGORIES_MAPPING.get(youtube_category)
        return iab_category


class PersistentSegmentChannelExportSerializer(Serializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Likes", "Dislikes", "Views", "Audited_Videos", "Overall_Score")

    # Fields map to segment export rows
    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default=None)
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
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
        score = map_brand_safety_score(obj.brand_safety.overall_score)
        return score

    def get_category(self, obj):
        youtube_category = getattr(obj.general_data, "top_category", "").lower()
        iab_category = YOUTUBE_TO_IAB_CATEGORIES_MAPPING.get(youtube_category)
        return iab_category
