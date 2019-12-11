from rest_framework.fields import CharField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.serializers import Serializer


class KeywordListExportSerializer(Serializer):
    keyword = CharField(source="main.id")
    search_volume = IntegerField(source="stats.search_volume")
    average_cpc = FloatField(source="stats.average_cpc")
    competition = FloatField(source="stats.competition")
    video_count = IntegerField(source="stats.video_count")
    views = IntegerField(source="stats.views")