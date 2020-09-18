from rest_framework.fields import CharField
from rest_framework.fields import DateTimeField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from brand_safety.languages import LANGUAGES
from channel.api.serializers.channel_export import ListExportSerializerMixin
from utils.brand_safety import map_brand_safety_score


class YTVideoLinkFromID(CharField):
    def to_representation(self, value):
        str_value = super(YTVideoLinkFromID, self).to_representation(value)
        return f"https://www.youtube.com/watch?v={str_value}"


class VideoListExportSerializer(ListExportSerializerMixin, Serializer):
    channel_id = CharField(source="channel.id")
    title = CharField(source="general_data.title")
    url = YTVideoLinkFromID(source="main.id")
    iab_categories = SerializerMethodField()
    language = SerializerMethodField()
    views = IntegerField(source="stats.views")
    monthly_views = IntegerField(source="stats.last_30day_views")
    weekly_views = IntegerField(source="stats.last_7day_views")
    daily_views = IntegerField(source="stats.last_day_views")
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

    def get_language(self, instance):
        try:
            lang_code = getattr(instance.general_data, "lang_code", "")
            language = LANGUAGES.get(lang_code) or lang_code
            return language
        except Exception:
            return ""

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError
