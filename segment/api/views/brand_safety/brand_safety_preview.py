
from django.core.paginator import Paginator
from django.core.paginator import EmptyPage
from django.http import Http404
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.views import APIView

from segment.models.persistent.constants import PersistentSegmentType
from segment.utils import get_persistent_segment_model_by_type
from segment.utils import get_persistent_segment_connector_config_by_type
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

    def get(self, request, **kwargs):
        """
        Provides paginated persistent segment preview data
        :param request:
            query_params: page (int), size (int)
        :param kwargs:
            segment_type (str) -> video / channel
        :return:
        """
        preview_type_id_mapping = {
            PersistentSegmentType.CHANNEL: "channel_id",
            PersistentSegmentType.VIDEO: "video_id"
        }
        page = request.query_params.get("page", 1)
        size = request.query_params.get("size", self.DEFAULT_PAGE_SIZE)
        segment_type = kwargs["segment_type"]
        item_id_key = preview_type_id_mapping.get(segment_type)
        if item_id_key is None:
            return Response(status=HTTP_400_BAD_REQUEST, data="Invalid segment type: {}".format(segment_type))
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
        # Helper function to set SDB connector config since Channel and Video SDB querying is similar
        config = get_persistent_segment_connector_config_by_type(segment_type, related_ids)
        if config is None:
            return Response(status=HTTP_400_BAD_REQUEST, data="Invalid segment type: {}".format(segment_type))
        connector_method = config.pop("method")
        response = connector_method(config)
        response_items = {
            item[item_id_key]: item for item in response.get("items")
        }
        preview_data = []
        for item in preview_page.object_list.values("related_id", "title", "category", "details", "thumbnail_image_url"):
            if response_items.get(item["related_id"]):
                # Map Cassandra item video_id, channel_id fields to just id
                data = response_items[item["related_id"]]
                item_id = data.pop(item_id_key)
                data["id"] = item_id
            else:
                # Map Postgres data to Cassandra structure
                data = self._map_segment_data(item, segment_type)
            preview_data.append(data)
        result = {
            "items": preview_data,
            "items_count": len(preview_data),
            "current_page": page,
            "max_page": paginator.num_pages,
        }
        return Response(status=HTTP_200_OK, data=result)

    @staticmethod
    def _map_segment_data(data, segment_type):
        """
        Maps Postgres persistent segment data to Singledb formatted data for client
        :param data: dict
        :param segment_type: str
        :return:
        """
        mapped_data = {
            "id": data["related_id"],
            "title": data.get("title"),
            "category": data.get("category"),
            "views": data["details"].get("views"),
            "likes": data["details"].get("likes"),
            "dislikes": data["details"].get("dislikes"),
            "language": data["details"].get("language"),
            "thumbnail_image_url": data.get("thumbnail_image_url")
        }
        if segment_type == PersistentSegmentType.CHANNEL:
            mapped_data["subscribers"] = data["details"].get("subscribers")
            mapped_data["audited_videos"] = data["details"].get("audited_videos")
        return mapped_data
