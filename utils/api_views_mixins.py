from django.db.models import Q

from segment.models import SegmentChannel, SegmentVideo


class SegmentFilterMixin(object):
    def _obtain_segment(self):
        """
        Try to get segment from db
        """
        channel_segment_id = self.request.query_params.get("channel_segment")
        video_segment_id = self.request.query_params.get("video_segment")
        segment_data = ((SegmentChannel, channel_segment_id),
                        (SegmentVideo, video_segment_id))
        for model, segment_id in segment_data:
            if not segment_id:
                continue
            try:
                if self.request.user.is_staff:
                    segment = model.objects.get(id=segment_id)
                else:
                    segment = model.objects.filter(
                        Q(owner=self.request.user) |
                        ~Q(category="private") |
                        Q(shared_with__contains=[self.request.user.email])
                    ).get(id=segment_id)
            except model.DoesNotExist:
                return None
            else:
                return segment
        return None

    def _validate_query_params(self):
        channel_segment_id = self.request.query_params.get("channel_segment")
        video_segment_id = self.request.query_params.get("video_segment")
        if all((channel_segment_id, video_segment_id)):
            return False, "'channel_segment_id' and 'video_segment_id'" \
                          " query params can't be use together"
        return True, ""
