from .base import BaseSegment
from .channel import SegmentChannel
from .channel import SegmentRelatedChannel
from .keyword import SegmentKeyword
from .keyword import SegmentRelatedKeyword
from .video import SegmentVideo
from .video import SegmentRelatedVideo


class ModelDoesNotExist(Exception):
    pass


@property
def SEGMENT_MODELS():
    return [m for m in BaseSegment.__subclasses__()]

@property
def SEGMENT_TYPES():
    return [m.segment_type for m in SEGMENT_MODELS.fget()]

def get_segment_model_by_type(segment_type):
    for model in SEGMENT_MODELS.fget():
        if model.segment_type == segment_type:
            return model
    raise ModelDoesNotExist("Invalid segment_type: %s" % segment_type)

def total_update_segments():
    SegmentVideo.objects.update_youtube_segments()
    SegmentKeyword.objects.update_youtube_segments()
    for model in SEGMENT_MODELS.fget():
        model.objects.update_statistics()
