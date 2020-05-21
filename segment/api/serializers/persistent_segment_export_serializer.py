"""
Segment api serializers module
"""
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField
from segment.api.serializers.segment_export_serializer_mixins import SegmentChannelExportSerializerMixin
from segment.api.serializers.segment_export_serializer_mixins import SegmentVideoExportSerializerMixin


class PersistentSegmentVideoExportSerializer(
    SegmentVideoExportSerializerMixin,
    Serializer
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


class PersistentSegmentChannelExportSerializer(
    SegmentChannelExportSerializerMixin,
    Serializer
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
