from rest_framework.serializers import BooleanField
from rest_framework.serializers import SerializerMethodField

from .custom_segment_export_serializers import CustomSegmentChannelWithMonetizationExportSerializer
from .custom_segment_export_serializers import CustomSegmentVideoExportSerializer


__all__ = [
    "CustomSegmentChannelVettedExportSerializer",
    "CustomSegmentVideoVettedExportSerializer",
]


class CustomSegmentChannelVettedExportSerializer(CustomSegmentChannelWithMonetizationExportSerializer):
    columns = ("URL", "Title", "Language", "Primary_Category", "Additional_Categories", "Subscribers",
               "Overall_Score", "Vetted", "Monetizable", "Brand_Safety",
               "Age_Group", "Gender", "Content_Type", "Content_Quality", "Num_Videos",
               "Vetting_Result", "Mismatched_Language", "Last_Vetted", "Vetted_By",
               "Country", "Monthly_Views",
               "Video_View_Rate", "Avg_CPV", "Avg_CPM", "Avg_CTR", "Avg_CTR_v", "Video_100_Completion_Rate",
               "Views_30_Days",
               )

    Vetting_Result = SerializerMethodField("get_vetting_result")
    Vetted_By = SerializerMethodField("get_vetted_by")

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError


class CustomSegmentVideoVettedExportSerializer(CustomSegmentVideoExportSerializer):
    columns = ("URL", "Title", "Language", "Primary_Category", "Additional_Categories", "Views", "Overall_Score",
               "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender",
               "Content_Type", "Content_Quality", "Vetting_Result", "Mismatched_Language",
               "Last_Vetted", "Vetted_By", "Country",
               "Video_View_Rate", "Avg_CPV", "Avg_CPM", "Avg_CTR", "Avg_CTR_v", "Video_100_Completion_Rate",
               "Views_30_Days",
               )

    Monetizable = BooleanField(source="monetization.is_monetizable", default=None)
    Vetting_Result = SerializerMethodField("get_vetting_result")
    Vetted_By = SerializerMethodField("get_vetted_by")

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError
