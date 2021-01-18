from segment.api.segment_list_adapter import SegmentListAPIViewAdapter
from segment.utils.utils import get_persistent_segment_model_by_type
from userprofile.constants import StaticPermissions
from utils.permissions import has_static_permission


class PersistentSegmentPreviewAPIView(SegmentListAPIViewAdapter):
    """
    Retrieve preview data for persistent segments
    """
    permission_classes = (
        has_static_permission(StaticPermissions.CTL__FEATURE_LIST),
    )

    @property
    def segment_model(self):
        segment_type = self.kwargs["segment_type"]
        segment_model = get_persistent_segment_model_by_type(segment_type)
        return segment_model
