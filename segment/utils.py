from segment.models.base import BaseSegment
from segment.models.keyword import SegmentKeyword
from segment.models.video import SegmentVideo
from segment.models.persistent.base import BasePersistentSegment


class ModelDoesNotExist(Exception):
    pass


@property
def SEGMENT_MODELS():
    return [m for m in BaseSegment.__subclasses__()]


@property
def SEGMENT_TYPES():
    return [m.segment_type for m in SEGMENT_MODELS.fget()]


@property
def PERSISTENT_SEGMENT_MODELS():
    return [m for m in BasePersistentSegment.__subclasses__()]


@property
def PERSISTENT_SEGMENT_TYPES():
    return [m.segment_type for m in PERSISTENT_SEGMENT_MODELS.fget()]


def get_segment_model_by_type(segment_type):
    for model in SEGMENT_MODELS.fget():
        if model.segment_type == segment_type:
            return model
    raise ModelDoesNotExist("Invalid segment_type: %s" % segment_type)


def total_update_segments(force_creation=False):
    SegmentVideo.objects.update_youtube_segments(force_creation=force_creation)
    SegmentKeyword.objects.update_youtube_segments(force_creation=force_creation)
    for model in SEGMENT_MODELS.fget():
        model.objects.update_statistics()


def get_persistent_segment_model_by_type(segment_type):
    for model in PERSISTENT_SEGMENT_MODELS.fget():
        if model.segment_type == segment_type:
            return model
    raise ModelDoesNotExist("Invalid segment_type: %s" % segment_type)
