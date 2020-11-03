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
from rest_framework.serializers import FloatField

from .segment_export_serializer_mixins import SegmentChannelExportSerializerMixin
from .segment_export_serializer_mixins import SegmentVideoExportSerializerMixin


class CustomSegmentChannelExportSerializer(SegmentChannelExportSerializerMixin, Serializer):
    columns = (
        "URL", "Title", "Language", "Primary_Category", "Additional_Categories", "Subscribers", "Overall_Score",
        "Vetted", "Brand_Safety", "Age_Group", "Gender", "Content_Type", "Content_Quality",
        "Num_Videos", "Mismatched_Language", "Last_Vetted", "Country", "Sentiment", "Monthly_Views",
        "Video_View_Rate", "Avg_CPV", "Avg_CPM", "Avg_CTR", "Avg_CTR_v", "Video_100_Completion_Rate", "Views_30_Days",
        "Last_Upload_Date",
    )

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Primary_Category = CharField(source="general_data.primary_category")
    Additional_Categories = SerializerMethodField("get_category")
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
    Sentiment = SerializerMethodField("get_sentiment")
    Video_View_Rate = FloatField(source="ads_stats.video_view_rate")
    Avg_CPV = FloatField(source="ads_stats.average_cpv")
    Avg_CPM = FloatField(source="ads_stats.average_cpm")
    Avg_CTR = FloatField(source="ads_stats.ctr")
    Avg_CTR_v = FloatField(source="ads_stats.ctr_v")
    Video_100_Completion_Rate = FloatField(source="ads_stats.video_quartile_100_rate")
    Views_30_Days = FloatField(source="stats.last_30day_views")
    Last_Upload_Date = DateTimeField(source="stats.last_video_published_at", format="%Y-%m-%d", default="")

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError


class CustomSegmentChannelWithMonetizationExportSerializer(CustomSegmentChannelExportSerializer):
    columns = (
        "URL", "Title", "Language", "Primary_Category", "Additional_Categories", "Subscribers", "Overall_Score",
        "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender",
        "Content_Type", "Content_Quality", "Num_Videos", "Mismatched_Language", "Last_Vetted",
        "Country", "Sentiment", "Monthly_Views", "Video_View_Rate", "Avg_CPV", "Avg_CPM", "Avg_CTR", "Avg_CTR_v",
        "Video_100_Completion_Rate", "Views_30_Days", "Last_Upload_Date",
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
        "URL", "Title", "Language", "Primary_Category", "Additional_Categories", "Views", "Monthly_Views",
        "Vetted", "Brand_Safety", "Age_Group", "Gender", "Content_Type", "Content_Quality", "Overall_Score",
        "Mismatched_Language", "Last_Vetted", "Country", "Sentiment",
        "Video_View_Rate", "Avg_CPV", "Avg_CPM", "Avg_CTR", "Avg_CTR_v", "Video_100_Completion_Rate", "Views_30_Days",
        "Upload_Date",
    )

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Primary_Category = CharField(source="general_data.primary_category")
    Additional_Categories = SerializerMethodField("get_category")
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
    Sentiment = SerializerMethodField("get_sentiment")
    Video_View_Rate = FloatField(source="ads_stats.video_view_rate")
    Avg_CPV = FloatField(source="ads_stats.average_cpv")
    Avg_CPM = FloatField(source="ads_stats.average_cpm")
    Avg_CTR = FloatField(source="ads_stats.ctr")
    Avg_CTR_v = FloatField(source="ads_stats.ctr_v")
    Video_100_Completion_Rate = FloatField(source="ads_stats.video_quartile_100_rate")
    Views_30_Days = FloatField(source="stats.last_30day_views")
    Upload_Date = DateTimeField(source="general_data.youtube_published_at", format="%Y-%m-%d", default="")

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError
