
from django.core.paginator import Paginator
from django.core.paginator import EmptyPage
from django.http import Http404
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.views import APIView

import brand_safety.constants as constants
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.constants import Sections
from segment.utils import get_persistent_segment_model_by_type
from utils.permissions import user_has_permission
from utils.brand_safety_view_decorator import get_brand_safety_data


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

        manager = ChannelManager(self.SECTIONS) if segment_type == constants.CHANNEL else VideoManager(self.SECTIONS)
        query = manager.ids_query(related_ids)
        data = manager.search(query).execute().hits
        preview_data = []
        for item in data:
            score = getattr(item.brand_safety, "overall_score", None)
            mapped = item.to_dict()
            mapped["brand_safety_data"] = get_brand_safety_data(score)
            preview_data.append(mapped)
        result = {
            "items": preview_data,
            "items_count": len(preview_data),
            "current_page": page,
            "max_page": paginator.num_pages
        }
        return Response(status=HTTP_200_OK, data=result)
