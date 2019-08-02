from rest_framework.fields import CharField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.permissions import IsAdminUser
from rest_framework.serializers import Serializer
from rest_framework_csv.renderers import CSVStreamingRenderer

from es_components.constants import Sections
from es_components.managers import KeywordManager
from utils.api.file_list_api_view import FileListApiView
from utils.datetime import time_instance
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class KeywordListExportSerializer(Serializer):
    keyword = CharField(source="main.id")
    search_volume = IntegerField(source="stats.search_volume")
    average_cpc = FloatField(source="stats.average_cpc")
    competition = FloatField(source="stats.competition")
    video_count = IntegerField(source="stats.video_count")
    views = IntegerField(source="stats.views")


class KeywordCSVRendered(CSVStreamingRenderer):
    header = [
        "keyword",
        "search_volume",
        "average_cpc",
        "competition",
        "video_count",
        "views",
    ]


class KeywordListExportApiView(FileListApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.keyword_list"),
            IsAdminUser
        ),
    )
    serializer_class = KeywordListExportSerializer
    renderer_classes = (KeywordCSVRendered,)

    @property
    def filename(self):
        now = time_instance.now()
        return "Keywords export report {}.csv".format(now.strftime("%Y-%m-%d_%H-%m"))

    def get_queryset(self):
        return ESQuerysetAdapter(KeywordManager((
            Sections.MAIN,
            Sections.STATS,
        )))

    def filter_queryset(self, queryset):
        ids_params = self.request.query_params.get("ids")
        if ids_params:
            ids = ids_params.split(",")
            query_filter = queryset.manager.ids_query(ids)
            queryset = queryset.filter(query_filter)
        return queryset
