from django.db.models import CharField
from django.db.models import Value
from rest_framework.generics import ListAPIView

from segment.api.paginator import SegmentPaginator
from segment.api.serializers.persistent_segment_serializer import PersistentSegmentSerializer
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentTitles
from segment.models.persistent.constants import PersistentSegmentType
from utils.permissions import user_has_permission


class PersistentMasterSegmentsListApiView(ListAPIView):
    serializer_class = PersistentSegmentSerializer
    pagination_class = SegmentPaginator
    permission_classes = (
        user_has_permission("userprofile.view_white_lists"),
    )

    def get_queryset(self):
        channels_segment_queryset = PersistentSegmentChannel.objects\
            .filter(title__in=[PersistentSegmentTitles.CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE,
                               PersistentSegmentTitles.CURATED_CHANNELS_MASTER_WHITELIST_SEGMENT_TITLE])\
            .annotate(segment_type=Value(PersistentSegmentType.CHANNEL, output_field=CharField()))

        videos_segment_queryset = PersistentSegmentVideo.objects\
            .filter(title=PersistentSegmentTitles.VIDEOS_MASTER_WHITELIST_SEGMENT_TITLE)\
            .annotate(segment_type=Value(PersistentSegmentType.VIDEO, output_field=CharField()))

        return videos_segment_queryset.union(channels_segment_queryset)
