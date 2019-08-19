from rest_framework.serializers import CharField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField


class CustomSegmentChannelExportSerializer(Serializer):
    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title")
    Language = CharField(source="brand_safety.language")
    Category = CharField(source="general_data.top_category")
    Subscribers = CharField(source="stats.subscribers")
    Overall_Score = CharField(source="brand_safety.overall_score")

    def get_url(self, obj):
        return f"https://www.youtube.com/channel/{obj.main.id}"


class CustomSegmentVideoExportSerializer(Serializer):
    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title")
    Language = CharField(source="brand_safety.language")
    Category = CharField(source="general_data.category")
    Views = CharField(source="stats.views")
    Overall_Score = CharField(source="brand_safety.overall_score")

    def get_url(self, obj):
        return f"https://www.youtube.com/watch?v={obj.main.id}"
