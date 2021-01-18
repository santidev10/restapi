from segment.api.segment_list_adapter import SegmentListAPIViewAdapter
from segment.models import CustomSegment
from userprofile.constants import StaticPermissions
from utils.permissions import has_static_permission


class SegmentPreviewAPIView(SegmentListAPIViewAdapter):
    """
    View to provide preview data for persistent segments
    """
    permission_classes = (
        has_static_permission(StaticPermissions.CTL),
    )

    @property
    def segment_model(self):
        return CustomSegment
