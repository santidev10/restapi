from math import floor

from rest_framework.serializers import CharField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField
from brand_safety.languages import LANGUAGES


class CustomSegmentChannelExportSerializer(Serializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Overall_Score")

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title")
    Language = SerializerMethodField("get_language")
    Category = CharField(source="general_data.top_category")
    Subscribers = CharField(source="stats.subscribers")
    Overall_Score = SerializerMethodField("get_overall_score")

    def get_url(self, obj):
        return f"https://www.youtube.com/channel/{obj.main.id}"

    def get_language(self, obj):
        brand_safety_language = getattr(obj.brand_safety, "language", None)
        if brand_safety_language == "all":
            language = "All"
        else:
            language = LANGUAGES.get(brand_safety_language, brand_safety_language)
        return language

    def get_overall_score(self, obj):
        score = floor((obj.brand_safety.overall_score or 0) / 10)
        return score


class CustomSegmentVideoExportSerializer(Serializer):
    columns = ("URL", "Title", "Language", "Category", "Views", "Overall_Score")

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title")
    Language = SerializerMethodField("get_language")
    Category = CharField(source="general_data.category")
    Views = CharField(source="stats.views")
    Overall_Score = SerializerMethodField("get_overall_score")

    def get_url(self, obj):
        return f"https://www.youtube.com/watch?v={obj.main.id}"

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
