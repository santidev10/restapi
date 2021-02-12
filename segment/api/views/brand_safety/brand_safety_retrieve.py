from rest_framework.generics import RetrieveAPIView

from segment.api.mixins import DynamicPersistentModelViewMixin
from segment.api.paginator import SegmentPaginator
from segment.api.serializers.persistent_segment_serializer import PersistentSegmentSerializer
from userprofile.constants import StaticPermissions


class PersistentSegmentRetrieveApiView(DynamicPersistentModelViewMixin, RetrieveAPIView):
    serializer_class = PersistentSegmentSerializer
    pagination_class = SegmentPaginator
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.BUILD__BSTL),
    )
