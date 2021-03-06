from rest_framework.serializers import BooleanField
from rest_framework.serializers import DateTimeField
from rest_framework.serializers import SerializerMethodField

from .custom_segment_export_serializers import AdminCustomSegmentChannelExportSerializer
from .custom_segment_export_serializers import AdminCustomSegmentVideoExportSerializer


__all__ = [
    "CustomSegmentChannelVettedExportSerializer",
    "CustomSegmentVideoVettedExportSerializer",
]


class CustomSegmentChannelVettedExportSerializer(AdminCustomSegmentChannelExportSerializer):
    columns = ("URL", "Title", "Language", "Primary_Category", "Additional_Categories", "Subscribers",
               "Overall_Score", "Vetted", "Monetizable", "Brand_Safety",
               "Age_Group", "Gender", "Content_Type", "Content_Quality", "Num_Videos",
               "Vetting_Result", "Mismatched_Language", "Last_Vetted", "Vetted_By",
               "Country", "Monthly_Views",
               "Video_View_Rate", "Avg_CPV", "Avg_CPM", "Avg_CTR", "Avg_CTR_v", "Video_100_Completion_Rate",
               "Views_30_Days", "IAS_Verified", "Last_Upload_Date",
               )

    IAS_Verified = DateTimeField(source="ias_data.ias_verified", format="%Y-%m-%d", default="")
    Vetting_Result = SerializerMethodField("get_vetting_result")
    Vetted_By = SerializerMethodField("get_vetted_by")

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError


class CustomSegmentVideoVettedExportSerializer(AdminCustomSegmentVideoExportSerializer):
    columns = ("URL", "Title", "Language", "Primary_Category", "Additional_Categories", "Views", "Overall_Score",
               "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender",
               "Content_Type", "Content_Quality", "Vetting_Result", "Mismatched_Language",
               "Last_Vetted", "Vetted_By", "Country",
               "Video_View_Rate", "Avg_CPV", "Avg_CPM", "Avg_CTR", "Avg_CTR_v", "Video_100_Completion_Rate",
               "Views_30_Days", "Upload_Date",
               )

    Monetizable = BooleanField(source="monetization.is_monetizable", default=None)
    Vetting_Result = SerializerMethodField("get_vetting_result")
    Vetted_By = SerializerMethodField("get_vetted_by")

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError
