from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField
from segment.api.serializers.segment_export_serializer_mixins import SegmentChannelExportSerializerMixin
from segment.api.serializers.segment_export_serializer_mixins import SegmentVideoExportSerializerMixin

"""
CustomSegment export serializers

Each columns tuple for all serializers are used as headers for export files
"""
class CustomSegmentChannelExportSerializer(
    SegmentChannelExportSerializerMixin,
    Serializer
):
    columns = (
        "URL", "Title", "Language", "Category", "Subscribers", "Overall_Score",
        "Vetted", "Brand_Safety", "Age_Group", "Gender", "Content_Type",
        "Num_Videos",
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
    Num_Videos = IntegerField(source="stats.total_videos_count")


class CustomSegmentChannelWithMonetizationExportSerializer(
    CustomSegmentChannelExportSerializer
):
    columns = (
        "URL", "Title", "Language", "Category", "Subscribers", "Overall_Score",
        "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender",
        "Content_Type", "Num_Videos",
    )

    Monetizable = BooleanField(source="monetization.is_monetizable", default=None)

    def __init__(self, instance, *args, **kwargs):
        super().__init__(instance, *args, **kwargs)


class CustomSegmentVideoExportSerializer(
    SegmentVideoExportSerializerMixin,
    Serializer
):
    columns = (
        "URL", "Title", "Language", "Category", "Views", "Overall_Score",
        "Vetted", "Brand_Safety", "Age_Group", "Gender", "Content_Type",
    )

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Views = IntegerField(source="stats.views")
    Overall_Score = SerializerMethodField("get_overall_score")
    Vetted = SerializerMethodField("get_vetted")
    Brand_Safety = SerializerMethodField("get_brand_safety")
    Age_Group = SerializerMethodField("get_age_group")
    Gender = SerializerMethodField("get_gender")
    Content_Type = SerializerMethodField("get_content_type")
