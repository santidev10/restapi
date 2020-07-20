"""
CustomSegment export serializers

Each columns tuple for all serializers are used as headers for export files
"""
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import DateTimeField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from segment.api.serializers.segment_export_serializer_mixins import SegmentChannelExportSerializerMixin
from segment.api.serializers.segment_export_serializer_mixins import SegmentVideoExportSerializerMixin


class CustomSegmentChannelExportSerializer(SegmentChannelExportSerializerMixin, Serializer):
    columns = (
        "URL", "Title", "Language", "Category", "Subscribers", "Overall_Score",
        "Vetted", "Brand_Safety", "Age_Group", "Gender", "Content_Type", "Content_Quality",
        "Num_Videos", "Mismatched_Language", "Last_Vetted", "Country", "Monthly_Views"
    )

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Subscribers = IntegerField(source="stats.subscribers")
    Overall_Score = SerializerMethodField("get_overall_score")
    Vetted = SerializerMethodField("get_vetted")
    Brand_Safety = SerializerMethodField("get_brand_safety")
    Age_Group = SerializerMethodField("get_age_group")
    Gender = SerializerMethodField("get_gender")
    Content_Type = SerializerMethodField("get_content_type")
    Content_Quality = SerializerMethodField("get_content_quality")
    Num_Videos = IntegerField(source="stats.total_videos_count")
    Mismatched_Language = SerializerMethodField("get_mismatched_language")
    Last_Vetted = DateTimeField(source="task_us_data.last_vetted_at", format="%Y-%m-%d", default="")
    Country = SerializerMethodField("get_country")
    Monthly_Views = IntegerField(source="stats.last_30day_views")

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError


class CustomSegmentChannelWithMonetizationExportSerializer(CustomSegmentChannelExportSerializer):
    columns = (
        "URL", "Title", "Language", "Category", "Subscribers", "Overall_Score",
        "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender",
        "Content_Type", "Content_Quality", "Num_Videos", "Mismatched_Language", "Last_Vetted",
        "Country", "Monthly_Views"
    )

    Monetizable = BooleanField(source="monetization.is_monetizable", default=None)

    def __init__(self, instance, *args, **kwargs):
        super().__init__(instance, *args, **kwargs)

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError


class CustomSegmentVideoExportSerializer(SegmentVideoExportSerializerMixin, Serializer):
    columns = (
        "URL", "Title", "Language", "Category", "Views", "Monthly_Views", "Overall_Score",
        "Vetted", "Brand_Safety", "Age_Group", "Gender", "Content_Type", "Content_Quality",
        "Mismatched_Language", "Last_Vetted", "Country"
    )

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Views = IntegerField(source="stats.views")
    Monthly_Views = IntegerField(source="stats.last_30day_views")
    Overall_Score = SerializerMethodField("get_overall_score")
    Vetted = SerializerMethodField("get_vetted")
    Brand_Safety = SerializerMethodField("get_brand_safety")
    Age_Group = SerializerMethodField("get_age_group")
    Gender = SerializerMethodField("get_gender")
    Content_Type = SerializerMethodField("get_content_type")
    Content_Quality = SerializerMethodField("get_content_quality")
    Mismatched_Language = SerializerMethodField("get_mismatched_language")
    Last_Vetted = DateTimeField(source="task_us_data.last_vetted_at", format="%Y-%m-%d", default="")
    Country = SerializerMethodField("get_country")

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError
