
from django.core.paginator import Paginator
from django.core.paginator import EmptyPage
from django.http import Http404
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.views import APIView

from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.constants import Sections
from segment.models.persistent.constants import PersistentSegmentType
from segment.utils import get_persistent_segment_model_by_type
from utils.permissions import user_has_permission


class PersistentSegmentPreviewAPIView(APIView):
    """
    View to provide preview data for persistent segments
    """
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )
    MAX_PAGE_SIZE = 10
    DEFAULT_PAGE_SIZE = 5
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS)

    def get(self, request, **kwargs):
        """
        Provides paginated persistent segment preview data
        :param request:
            query_params: page (int), size (int)
        :param kwargs:
            segment_type (str) -> video / channel
        :return:
        """
        segment_type = kwargs["segment_type"]
        preview_config = {
            PersistentSegmentType.CHANNEL: {
                "manager": ChannelManager(self.SECTIONS),
                "mapper": self._map_channel_data
            },
            PersistentSegmentType.VIDEO: {
                "manager": VideoManager(self.SECTIONS),
                "mapper": self._map_video_data
            }
        }
        config = preview_config[segment_type]
        page = request.query_params.get("page", 1)
        size = request.query_params.get("size", self.DEFAULT_PAGE_SIZE)
        try:
            page = int(page)
        except ValueError:
            return Response(status=HTTP_400_BAD_REQUEST, data="Invalid page number: {}".format(page))
        try:
            size = int(size)
        except ValueError:
            return Response(status=HTTP_400_BAD_REQUEST, data="Invalid page size number: {}".format(page))
        if page <= 0:
            page = 1
        if size <= 0:
            size = self.DEFAULT_PAGE_SIZE
        elif size >= self.MAX_PAGE_SIZE:
            size = self.MAX_PAGE_SIZE
        segment_model = get_persistent_segment_model_by_type(segment_type)
        try:
            segment = segment_model.objects.get(id=kwargs["pk"])
        except segment_model.DoesNotExist:
            raise Http404
        related_items = segment.related.select_related("segment").order_by("related_id")
        paginator = Paginator(related_items, size)
        try:
            preview_page = paginator.page(page)
        except EmptyPage:
            page = paginator.num_pages
            preview_page = paginator.page(page)
        related_ids = [item.related_id for item in preview_page.object_list]

        manager = config["manager"]
        query = manager.ids_query(related_ids)
        data = manager.search(query).execute().hits
        preview_data = [config["mapper"](item) for item in data]
        result = {
            "items": preview_data,
            "items_count": len(preview_data),
            "current_page": page,
            "max_page": paginator.num_pages
        }
        return Response(status=HTTP_200_OK, data=result)

    def _map_channel_data(self, data):
        """
        Map Elasticsearch video data
        :param data: Elasticsearch dsl Attrdict
        :return: dict
        """
        mapped = {
            "id": data.main.id,
            "title": getattr(data.general_data, "title", ""),
            "category": getattr(data.general_data, "top_category", ""),
            "views": getattr(data.stats, "views", ""),
            "likes": getattr(data.stats, "observed_videos_likes", ""),
            "dislikes": getattr(data.stats, "observed_videos_dislikes", ""),
            "language": getattr(data.general_data, "top_language", ""),
            "thumbnail_image_url": getattr(data.general_data, "thumbnail_image_url", ""),
        }
        return mapped

    def _map_video_data(self, data):
        """
        Map Elasticsearch channel data
        :param data: Elasticsearch dsl Attrdict
        :return: dict
        """
        mapped = {
            "id": data.main.id,
            "title": getattr(data.general_data, "title", ""),
            "category": getattr(data.general_data, "category", ""),
            "views": getattr(data.stats, "views", ""),
            "likes": getattr(data.stats, "likes", ""),
            "dislikes": getattr(data.stats, "dislikes", ""),
            "language": getattr(data.general_data, "language", ""),
            "thumbnail_image_url": getattr(data.general_data, "thumbnail_image_url", ""),
        }
        return mapped

