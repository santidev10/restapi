from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from brand_safety.languages import LANGUAGES
from es_components.iab_categories import YOUTUBE_TO_IAB_CATEGORIES_MAPPING
from utils.brand_safety import map_brand_safety_score


class CustomSegmentChannelExportSerializer(Serializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Overall_Score")

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Subscribers = IntegerField(source="stats.subscribers")
    Overall_Score = SerializerMethodField("get_overall_score")

    def get_url(self, obj):
        return f"https://www.youtube.com/channel/{obj.main.id}"

    def get_language(self, obj):
        brand_safety_language = getattr(obj.brand_safety, "language", "") or ""
        if brand_safety_language == "all":
            language = "All"
        else:
            language = LANGUAGES.get(brand_safety_language, brand_safety_language)
        return language

    def get_overall_score(self, obj):
        score = map_brand_safety_score(obj.brand_safety.overall_score)
        return score

    def get_category(self, obj):
        youtube_category = (getattr(obj.general_data, "top_category", "") or "").lower()
        try:
            iab_category = YOUTUBE_TO_IAB_CATEGORIES_MAPPING.get(youtube_category)[-1]
        except Exception as e:
            iab_category = ""
        return iab_category


class CustomSegmentVideoExportSerializer(Serializer):
    columns = ("URL", "Title", "Language", "Category", "Views", "Overall_Score")

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Views = IntegerField(source="stats.views")
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
        score = map_brand_safety_score(obj.brand_safety.overall_score)
        return score

    def get_category(self, obj):
        youtube_category = (getattr(obj.general_data, "category", "") or "").lower()
        try:
            iab_category = YOUTUBE_TO_IAB_CATEGORIES_MAPPING.get(youtube_category)[-1]
        except Exception as e:
            iab_category = ""
        return iab_category
