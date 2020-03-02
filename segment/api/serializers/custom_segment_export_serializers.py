from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from brand_safety.languages import LANGUAGES
from utils.brand_safety import map_brand_safety_score

"""
CustomSegment export serializers

Each columns tuple for all serializers are used as headers for export files
"""


class CustomSegmentChannelExportSerializer(Serializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Overall_Score", "Vetted")

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Subscribers = IntegerField(source="stats.subscribers")
    Overall_Score = SerializerMethodField("get_overall_score")
    Vetted = SerializerMethodField("get_vetted")

    def __init__(self, instance, *args, **kwargs):
        self.extra_data = kwargs.pop("extra_data", {})
        super().__init__(instance, *args, **kwargs)

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
        categories = getattr(obj.general_data, "iab_categories", []) or []
        joined = ", ".join(categories)
        return joined

    def get_vetted(self, obj):
        vetted = "Y" if getattr(obj.task_us_data, "created_at", None) is not None else None
        return vetted


class CustomSegmentChannelWithMonetizationExportSerializer(CustomSegmentChannelExportSerializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Overall_Score", "Vetted", "Monetizable")

    Monetizable = BooleanField(source="monetization.is_monetizable", default=None)

    def __init__(self, instance, *args, **kwargs):
        super().__init__(instance, *args, **kwargs)


class CustomSegmentVideoExportSerializer(Serializer):
    columns = ("URL", "Title", "Language", "Category", "Views", "Overall_Score")

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Views = IntegerField(source="stats.views")
    Overall_Score = SerializerMethodField("get_overall_score")

    def __init__(self, instance, *args, **kwargs):
        self.extra_data = kwargs.pop("extra_data", {})
        super().__init__(instance, *args, **kwargs)

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
        categories = getattr(obj.general_data, "iab_categories", []) or []
        joined = ", ".join(categories)
        return joined
