from rest_framework.serializers import BooleanField
from rest_framework.serializers import SerializerMethodField

from segment.api.serializers.custom_segment_export_serializers import \
    CustomSegmentChannelWithMonetizationExportSerializer
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentVideoExportSerializer


class CustomSegmentChannelVettedExportSerializer(CustomSegmentChannelWithMonetizationExportSerializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers",
               "Overall_Score", "Vetted", "Monetizable", "Brand_Safety",
               "Age_Group", "Gender", "Content_Type", "Content_Quality", "Num_Videos",
               "Vetting_Result", "Mismatched_Language", "Last_Vetted",
               "Country", "Monthly_Views",)

    Vetting_Result = SerializerMethodField("get_vetting_result")

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError


class CustomSegmentVideoVettedExportSerializer(CustomSegmentVideoExportSerializer):
    columns = ("URL", "Title", "Language", "Category", "Views", "Overall_Score",
               "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender",
               "Content_Type", "Content_Quality", "Vetting_Result", "Mismatched_Language",
               "Last_Vetted", "Country",)

    Monetizable = BooleanField(source="monetization.is_monetizable", default=None)
    Vetting_Result = SerializerMethodField("get_vetting_result")

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError
