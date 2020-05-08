from rest_framework.serializers import BooleanField
from rest_framework.serializers import SerializerMethodField
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentChannelWithMonetizationExportSerializer
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentVideoExportSerializer


class CustomSegmentChannelVettedExportSerializer(
    CustomSegmentChannelWithMonetizationExportSerializer
):
    columns = ("URL", "Title", "Language", "Category", "Subscribers",
               "Overall_Score", "Vetted", "Monetizable", "Brand_Safety",
               "Age_Group", "Gender", "Content_Type", "Num_Videos",
               "Vetting_Result")

    Vetting_Result = SerializerMethodField("get_vetting_result")


class CustomSegmentVideoVettedExportSerializer(
    CustomSegmentVideoExportSerializer
):
    columns = ("URL", "Title", "Language", "Category", "Views", "Overall_Score",
               "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender",
               "Content_Type", "Vetting_Result")

    Monetizable = BooleanField(source="monetization.is_monetizable", default=None)
    Vetting_Result = SerializerMethodField("get_vetting_result")

