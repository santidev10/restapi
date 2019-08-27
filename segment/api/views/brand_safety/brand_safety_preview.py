from django.http import Http404
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.views import APIView

import brand_safety.constants as constants
from channel.api.serializers.channel_with_blacklist_data import ChannelWithBlackListSerializer
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.constants import Sections
from es_components.constants import SortDirections
from segment.utils import get_persistent_segment_model_by_type
from video.api.serializers.video_with_blacklist_data import VideoWithBlackListSerializer
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
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)

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
        page = request.query_params.get("page", 1)
        size = request.query_params.get("size", self.DEFAULT_PAGE_SIZE)
        segment_model = get_persistent_segment_model_by_type(segment_type)

        if segment_type == constants.CHANNEL:
            es_manager = ChannelManager(self.SECTIONS)
            sort_key = {"stats.subscribers": {"order": SortDirections.DESCENDING}}
            serializer = ChannelWithBlackListSerializer
        else:
            es_manager = VideoManager(self.SECTIONS)
            sort_key = {"stats.views": {"order": SortDirections.DESCENDING}}
            serializer = VideoWithBlackListSerializer
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
        try:
            segment = segment_model.objects.get(id=kwargs["pk"])
        except segment_model.DoesNotExist:
            raise Http404

        max_items = segment.details["items_count"]
        max_page = (max_items // size) or 1
        if page > max_page:
            page = max_page

        offset = (page - 1) * size
        query = segment.get_segment_items_query()
        result = es_manager.search(query, sort=sort_key, offset=offset, limit=offset + size).execute()
        data = serializer(result.hits._l_, many=True).data
        result = {
            "items": data,
            "items_count": len(data),
            "current_page": page,
            "max_page": max_page
        }
        return Response(status=HTTP_200_OK, data=result)
