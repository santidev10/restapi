from rest_framework.serializers import BooleanField
from rest_framework.serializers import SerializerMethodField
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentChannelWithMonetizationExportSerializer
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentVideoExportSerializer


class CustomSegmentChannelVettedExportSerializer(CustomSegmentChannelWithMonetizationExportSerializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Overall_Score", "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender", "Content_Type")

    Brand_Safety = SerializerMethodField("get_brand_safety")
    Language = SerializerMethodField("get_language")
    Content_Type = SerializerMethodField("get_content_type")
    Gender = SerializerMethodField("get_gender")
    Age_Group = SerializerMethodField("get_age_group")


class CustomSegmentVideoVettedExportSerializer(CustomSegmentVideoExportSerializer):
    columns = ("URL", "Title", "Language", "Category", "Views", "Overall_Score", "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender", "Content_Type")

    Monetizable = BooleanField(source="monetization.is_monetizable", default=None)
    Brand_Safety = SerializerMethodField("get_brand_safety")
    Language = SerializerMethodField("get_language")
    Content_Type = SerializerMethodField("get_content_type")
    Gender = SerializerMethodField("get_gender")
    Age_Group = SerializerMethodField("get_age_group")
    Vetted = SerializerMethodField("get_vetted")

