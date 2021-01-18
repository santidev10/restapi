import operator

from rest_framework.response import Response
from rest_framework.views import APIView

from segment.api.serializers import CTLSerializer
from segment.api.serializers import CTLWithoutDownloadUrlSerializer
from segment.models import CustomSegment
from segment.models.constants import SegmentTypeEnum
from userprofile.constants import StaticPermissions
from utils.permissions import has_static_permission

MINIMUM_ITEMS_COUNT = 100


class CustomSegmentListApiView(APIView):
    permission_classes = (
        has_static_permission(StaticPermissions.CTL__FEATURE_LIST),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, *kwargs)
        self.data = {}

    def get(self, request, segment_type):
        """
        Should replicate the output of PersistentSegmentListApiView's GET list view.
        """
        featured_segments = CustomSegment.objects.filter(
            is_featured=True,
            segment_type=self.get_segment_type_id(segment_type)
        )
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
            serializer_class = self.get_custom_segment_serializer_class()
            serializer = serializer_class(instance=segment)
            serialized.append(serializer.data)
        self.data['items'] = serialized

    def get_custom_segment_serializer_class(self):
        if self.request.user.has_perm('userprofile.download_audit'):
            return CTLSerializer
        return CTLWithoutDownloadUrlSerializer

    def get_segment_type_id(self, segment_type):
        """
        maps string segment_type to segment id eg: 'channel' to 1
        """
        return SegmentTypeEnum[segment_type.upper()].value
