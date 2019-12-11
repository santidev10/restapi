from segment.api.segment_list_adapter import SegmentListAPIViewAdapter
from segment.utils import get_persistent_segment_model_by_type
from utils.permissions import user_has_permission


class PersistentSegmentPreviewAPIView(SegmentListAPIViewAdapter):
    """
    View to provide preview data for persistent segments
    """
    permission_classes = (
        user_has_permission("userprofile.view_audit_segments"),
    )

    @property
    def segment_model(self):
        segment_type = self.kwargs["segment_type"]
        segment_model = get_persistent_segment_model_by_type(segment_type)
        return segment_model
