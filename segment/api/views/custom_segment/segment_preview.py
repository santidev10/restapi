from segment.api.segment_list_adapter import SegmentListAPIViewAdapter
from segment.models import CustomSegment
from userprofile.constants import StaticPermissions


class SegmentPreviewAPIView(SegmentListAPIViewAdapter):
    """
    View to provide preview data for persistent segments
    """
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.CTL),
    )

    @property
    def segment_model(self):
        return CustomSegment
