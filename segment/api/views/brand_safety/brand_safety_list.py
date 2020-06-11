from django.db.models import Q
from rest_framework.generics import ListAPIView
from rest_framework.response import Response
from rest_framework.views import APIView
from segment.api.mixins import DynamicPersistentModelViewMixin
from segment.api.paginator import SegmentPaginator
from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.api.serializers.persistent_segment_serializer import PersistentSegmentSerializer
from segment.models import CustomSegment
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from segment.utils.utils import get_persistent_segment_model_by_type
from userprofile.utils import is_correct_apex_domain
from utils.permissions import user_has_permission

MINIMUM_ITEMS_COUNT = 100


class CustomSegmentListApiView(APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, *kwargs)
        self.data = {}
        self.segment_type_map = CustomSegment.segment_type_to_id

    def get(self, request, segment_type):
        """
        Should replicate the output of PersistentSegmentListApiView's GET list view.
        """
        persistent_segment_model = get_persistent_segment_model_by_type(segment_type)
        master_lists = persistent_segment_model.objects.filter(is_master=True)
        featured_segments = CustomSegment.objects.filter(
            is_featured=True,
            segment_type=self.get_segment_type_id(segment_type)
        )
        self.add_master_list_nodes(master_lists)
        self.add_featured_segments_node(featured_segments)
        return Response(data=self.data, status=200)

    def add_featured_segments_node(self, featured_segments):
        """
        adds serialized CustomSegments to the 'items' node in the response
        excludes segments whose item count is less than MINIMUM_ITEMS_COUNT
        """
        serialized = []
        for segment in featured_segments:
            if not segment.statistics \
                or segment.statistics.get('items_count', 0) < MINIMUM_ITEMS_COUNT:
                continue
            serializer = CustomSegmentSerializer(instance=segment)
            serialized.append(serializer.data)
        self.data['items'] = serialized

    def get_segment_type_id(self, segment_type):
        """
        maps string segment_type to segment id eg: 'channel' to 1
        """
        return self.segment_type_map[segment_type]

    def add_master_list_nodes(self, master_lists):
        """
        adds master list node to the response data
        """
        for segment in master_lists:
            serializer = PersistentSegmentSerializer(instance=segment)
            if PersistentSegmentCategory.WHITELIST in segment.category:
                self.data['master_whitelist'] = serializer.data
            elif PersistentSegmentCategory.BLACKLIST in segment.category:
                self.data['master_blacklist'] = serializer.data


class PersistentSegmentListApiView(DynamicPersistentModelViewMixin, ListAPIView):
    serializer_class = PersistentSegmentSerializer
    pagination_class = SegmentPaginator
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )

    def get_queryset(self):
        """
        Filter queryset depending on APEX request HTTP_ORIGIN or HTTP_REFERER

        :return: Queryset
        """
        request_origin = self.request.META.get("HTTP_ORIGIN", "") or self.request.META.get("HTTP_REFERER", "")
        if is_correct_apex_domain(request_origin):
            queryset = super().get_queryset().filter(Q(category=PersistentSegmentCategory.APEX) | Q(is_master=True))
        else:
            queryset = super().get_queryset().filter(Q(category=PersistentSegmentCategory.WHITELIST) | Q(is_master=True))
        return queryset

    def finalize_response(self, request, response, *args, **kwargs):
        """
        Format data for response and excludes items with items_count values less than threshold
        :return: Response
        """
        data = {
            "master_blacklist": None,
            "master_whitelist": None,
            "items": []
        }
        for item in response.data.get("items", []):
            if (not item.get("statistics") or item["statistics"].get("items_count", 0) < MINIMUM_ITEMS_COUNT) and item["is_master"] is False:
                continue
            if item["category"] == PersistentSegmentCategory.WHITELIST and item["is_master"] is True:
                data["master_whitelist"] = item
            elif item["category"] == PersistentSegmentCategory.BLACKLIST and item["is_master"] is True:
                data["master_blacklist"] = item
            else:
                # remove "Channels " or "Videos " prefix
                prefix = "{}s ".format(item.get("segment_type").capitalize())
                if item.get("title", prefix).startswith(prefix):
                    item["title"] = item.get("title", "")[len(prefix):]
                if not item.get("thumbnail_image_url"):
                    item["thumbnail_image_url"] = S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
                data["items"].append(item)
        response.data = data
        return super().finalize_response(request, response, *args, **kwargs)
