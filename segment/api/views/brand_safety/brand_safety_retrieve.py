from rest_framework.generics import RetrieveAPIView

from segment.api.mixins import DynamicPersistentModelViewMixin
from segment.api.paginator import SegmentPaginator
from segment.api.serializers.persistent_segment_serializer import PersistentSegmentSerializer
from userprofile.constants import StaticPermissions
from utils.permissions import has_static_permission


class PersistentSegmentRetrieveApiView(DynamicPersistentModelViewMixin, RetrieveAPIView):
    serializer_class = PersistentSegmentSerializer
    pagination_class = SegmentPaginator
    permission_classes = (
        has_static_permission(StaticPermissions.CTL__FEATURE_LIST),
    )
