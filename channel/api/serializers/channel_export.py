from math import floor

from rest_framework.fields import CharField
from rest_framework.fields import DateTimeField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from utils.api.fields import CharFieldListBased


class YTChannelLinkFromID(CharField):
    def to_representation(self, value):
        str_value = super(YTChannelLinkFromID, self).to_representation(value)
        return f"https://www.youtube.com/channel/{str_value}/"


class ChannelListExportSerializer(Serializer):
    title = CharField(source="general_data.title")
    url = YTChannelLinkFromID(source="main.id")
    country = CharField(source="general_data.country")
    category = CharField(source="general_data.top_category")
    emails = CharFieldListBased(source="general_data.emails")
    subscribers = IntegerField(source="stats.subscribers")
    thirty_days_subscribers = IntegerField(source="stats.last_30day_subscribers")
    thirty_days_views = IntegerField(source="stats.last_30day_views")
    views_per_video = FloatField(source="stats.views_per_video")
    sentiment = FloatField(source="stats.sentiment")
    engage_rate = FloatField(source="stats.engage_rate")
    last_video_published_at = DateTimeField(source="stats.last_video_published_at")
    video_view_rate = FloatField(source="ads_stats.video_view_rate")
    ctr = FloatField(source="ads_stats.ctr")
    ctr_v = FloatField(source="ads_stats.ctr_v")
    average_cpv = FloatField(source="ads_stats.average_cpv")
    brand_safety_score = SerializerMethodField()

    def get_brand_safety_score(self, doc):
        overall_score = doc.brand_safety.overall_score
        if overall_score:
            score = floor(doc.brand_safety.overall_score / 10)
        else:
            score = overall_score
        return score
