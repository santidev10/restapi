from segment.api.segment_list_adapter import SegmentListAPIViewAdapter
from segment.models import CustomSegment
from utils.permissions import user_has_permission


class SegmentPreviewAPIView(SegmentListAPIViewAdapter):
    """
    View to provide preview data for persistent segments
    """
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )

    @property
    def segment_model(self):
        return CustomSegment

