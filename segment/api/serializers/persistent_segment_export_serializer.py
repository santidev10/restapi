"""
Segment api serializers module
"""
from brand_safety.languages import LANGUAGES
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import SerializerMethodField
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentChannelExportSerializer
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentVideoExportSerializer
from utils.brand_safety import map_brand_safety_score


class PersistentSegmentVideoExportSerializer(
    CustomSegmentVideoExportSerializer
):
    columns = ("URL", "Title", "Language", "Category", "Likes", "Dislikes",
               "Views", "Overall_Score", "Vetted", "Brand_Safety", "Age_Group",
               "Gender", "Content_Type")

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Likes = IntegerField(source="stats.likes")
    Dislikes = IntegerField(source="stats.dislikes")
    Views = IntegerField(source="stats.views")
    Overall_Score = SerializerMethodField("get_overall_score")
    Vetted = SerializerMethodField("get_vetted")
    Brand_Safety = SerializerMethodField("get_brand_safety")
    Age_Group = SerializerMethodField("get_age_group")
    Gender = SerializerMethodField("get_gender")
    Content_Type = SerializerMethodField("get_content_type")

    def get_url(self, obj):
        return f"https://www.youtube.com/video/{obj.main.id}"

    def get_language(self, obj):
        lang_code = getattr(obj.general_data, "lang_code", "")
        language = LANGUAGES.get(lang_code, lang_code)
        return language

    def get_overall_score(self, obj):
        score = map_brand_safety_score(obj.brand_safety.overall_score)
        return score


class PersistentSegmentChannelExportSerializer(
    CustomSegmentChannelExportSerializer
):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Likes",
               "Dislikes", "Views", "Audited_Videos", "Overall_Score", "Vetted",
               "Brand_Safety", "Age_Group", "Gender", "Content_Type")

    # Fields map to segment export rows
    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Subscribers = IntegerField(source="stats.subscribers")
    Likes = IntegerField(source="stats.observed_videos_likes")
    Dislikes = IntegerField(source="stats.observed_videos_dislikes")
    Views = IntegerField(source="stats.views")
    Audited_Videos = IntegerField(source="brand_safety.videos_scored")
    Overall_Score = SerializerMethodField("get_overall_score")
    Vetted = SerializerMethodField('get_vetted')
    Brand_Safety = SerializerMethodField("get_brand_safety")
    Age_Group = SerializerMethodField("get_age_group")
    Gender = SerializerMethodField("get_gender")
    Content_Type = SerializerMethodField("get_content_type")

    def get_url(self, obj):
        return f"https://www.youtube.com/channel/{obj.main.id}"

    def get_language(self, obj):
        lang_code = getattr(obj.general_data, "top_lang_code", "")
        language = LANGUAGES.get(lang_code, lang_code)
        return language

    def get_overall_score(self, obj):
        score = map_brand_safety_score(obj.brand_safety.overall_score)
        return score
