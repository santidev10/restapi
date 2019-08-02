from django.conf import settings
from rest_framework.fields import CharField
from rest_framework.fields import DateTimeField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.permissions import IsAdminUser
from rest_framework.serializers import Serializer
from rest_framework_csv.renderers import CSVStreamingRenderer

from es_components.constants import Sections
from es_components.managers import VideoManager
from utils.api.file_list_api_view import FileListApiView
from utils.brand_safety_view_decorator import get_brand_safety_items
from utils.datetime import time_instance
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from utils.serializers.fields import ParentDictValueField


class YTVideoLinkFromID(CharField):
    def to_representation(self, value):
        str_value = super(YTVideoLinkFromID, self).to_representation(value)
        return f"https://www.youtube.com/watch?v={str_value}"


class VideoListExportSerializer(Serializer):
    title = CharField(source="general_data.title")
    url = YTVideoLinkFromID(source="main.id")
    views = IntegerField(source="stats.views")
    likes = IntegerField(source="stats.likes")
    dislikes = IntegerField(source="stats.dislikes")
    comments = IntegerField(source="stats.comments")
    youtube_published_at = DateTimeField(source="general_data.youtube_published_at")
    brand_safety_score = ParentDictValueField("brand_safety_scores", source="main.id", property_key="overall_score")
    video_view_rate = FloatField(source="ads_stats.video_view_rate")
    ctr = FloatField(source="ads_stats.ctr")
    ctr_v = FloatField(source="ads_stats.ctr_v")
    average_cpv = FloatField(source="ads_stats.average_cpv")

    def __init__(self, instance, *args, **kwargs):
        super(VideoListExportSerializer, self).__init__(instance, *args, **kwargs)
        self.brand_safety_scores = {}
        if instance:
            items = instance if isinstance(instance, list) else [instance]
            ids = [item.main.id for item in items]
            self.brand_safety_scores = get_brand_safety_items(ids, settings.BRAND_SAFETY_VIDEO_INDEX)


class VideoCSVRendered(CSVStreamingRenderer):
    header = [
        "title",
        "url",
        "views",
        "likes",
        "dislikes",
        "comments",
        "youtube_published_at",
        "brand_safety_score",
        "video_view_rate",
        "ctr",
        "ctr_v",
        "average_cpv",
    ]


class VideoListExportApiView(FileListApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.video_list"),
            IsAdminUser
        ),
    )
    serializer_class = VideoListExportSerializer
    renderer_classes = (VideoCSVRendered,)

    @property
    def filename(self):
        now = time_instance.now()
        return "Videos export report {}.csv".format(now.strftime("%Y-%m-%d_%H-%m"))

    def get_queryset(self):
        return ESQuerysetAdapter(VideoManager((
            Sections.MAIN,
            Sections.GENERAL_DATA,
            Sections.STATS,
            Sections.ADS_STATS,
        )))

    def filter_queryset(self, queryset):
        ids_params = self.request.query_params.get("ids")
        if ids_params:
            ids = ids_params.split(",")
            query_filter = queryset.manager.ids_query(ids)
            queryset = queryset.filter(query_filter)
        return queryset