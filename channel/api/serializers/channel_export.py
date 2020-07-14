from rest_framework.fields import CharField
from rest_framework.fields import DateTimeField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from brand_safety.languages import LANGUAGES
from utils.brand_safety import map_brand_safety_score


class YTChannelLinkFromID(CharField):
    def to_representation(self, value):
        str_value = super(YTChannelLinkFromID, self).to_representation(value)
        return f"https://www.youtube.com/channel/{str_value}"


class ChannelListExportSerializer(Serializer):
    title = CharField(source="general_data.title")
    url = YTChannelLinkFromID(source="main.id")
    country = CharField(source="general_data.country")
    language = SerializerMethodField()
    iab_categories = SerializerMethodField()
    subscribers = IntegerField(source="stats.subscribers")
    thirty_days_subscribers = IntegerField(source="stats.last_30day_subscribers")
    views = IntegerField(source="stats.views")
    monthly_views = IntegerField(source="stats.last_30day_views")
    weekly_views = IntegerField(source="stats.last_7day_views")
    daily_views = IntegerField(source="stats.last_day_views")
    views_per_video = FloatField(source="stats.views_per_video")
    sentiment = FloatField(source="stats.sentiment")
    engage_rate = FloatField(source="stats.engage_rate")
    last_video_published_at = DateTimeField(source="stats.last_video_published_at")
    video_view_rate = FloatField(source="ads_stats.video_view_rate")
    ctr = FloatField(source="ads_stats.ctr")
    ctr_v = FloatField(source="ads_stats.ctr_v")
    average_cpv = FloatField(source="ads_stats.average_cpv")
    brand_safety_score = SerializerMethodField()

    def get_iab_categories(self, instance):
        iab_categories = getattr(instance.general_data, "iab_categories", [])
        return ", ".join(iab_categories)

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError

    def get_brand_safety_score(self, doc):
        score = map_brand_safety_score(doc.brand_safety.overall_score)
        return score

    def get_language(self, instance):
        try:
            lang_code = getattr(instance.general_data, "top_lang_code", "")
            language = LANGUAGES.get(lang_code) or lang_code
            return language
        except Exception:
            return ""
