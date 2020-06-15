import operator
from rest_framework.response import Response
from rest_framework.views import APIView
from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.api.serializers.persistent_segment_serializer import PersistentSegmentSerializer
from segment.models import CustomSegment
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.utils.utils import get_persistent_segment_model_by_type
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
        featured_segments = self.sort_featured_segments(featured_segments)
        self.add_featured_segments_node(featured_segments)
        return Response(data=self.data, status=200)

    def sort_featured_segments(self, featured_segments):
        """
        sort the featured_segments query without using the ORM, for efficiency
        """
        get_key = operator.attrgetter('title')
        sorted_segments = sorted(
            featured_segments,
            key=lambda s: get_key(s).lower(),
            reverse=False
        )
        return sorted_segments

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
