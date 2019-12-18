from rest_framework.fields import CharField
from rest_framework.fields import DateTimeField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from utils.brand_safety import map_brand_safety_score


class YTVideoLinkFromID(CharField):
    def to_representation(self, value):
        str_value = super(YTVideoLinkFromID, self).to_representation(value)
        return f"https://www.youtube.com/watch?v={str_value}"


class VideoListExportSerializer(Serializer):
    title = CharField(source="general_data.title")
    url = YTVideoLinkFromID(source="main.id")
    iab_categories = CharField(source="general_data.iab_categories")
    views = IntegerField(source="stats.views")
    likes = IntegerField(source="stats.likes")
    dislikes = IntegerField(source="stats.dislikes")
    comments = IntegerField(source="stats.comments")
    youtube_published_at = DateTimeField(source="general_data.youtube_published_at")
    video_view_rate = FloatField(source="ads_stats.video_view_rate")
    ctr = FloatField(source="ads_stats.ctr")
    ctr_v = FloatField(source="ads_stats.ctr_v")
    average_cpv = FloatField(source="ads_stats.average_cpv")
    brand_safety_score = SerializerMethodField()

    def get_brand_safety_score(self, doc):
        score = map_brand_safety_score(doc.brand_safety.overall_score)
        return score
