from segment.models.base import BaseSegment
from segment.models.keyword import SegmentKeyword
from segment.models.video import SegmentVideo
from segment.models.persistent.base import BasePersistentSegment
from segment.models.persistent.constants import PERSISTENT_SEGMENT_CHANNEL_PREVIEW_FIELDS
from segment.models.persistent.constants import PERSISTENT_SEGMENT_VIDEO_PREVIEW_FIELDS
from singledb.connector import SingleDatabaseApiConnector as Connector


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


def get_persistent_segment_preview_data(page, method, params):
    response = method(params)
    result = {
        'items': response["items"],
        'current_page': page,
    }
    return result


def get_persistent_segment_connector_config_by_type(segment_type, related_ids):
    """
    Helper method to retrieve SDB data based on segment type
    :param segment_type: Segment type, e.g. channel or video
    :param related_ids: Channel or video ids to query sdb for
    :return: dict
    """
    try:
        iter(related_ids)
        related_ids = ",".join(related_ids)
    except TypeError:
        pass
    valid_segment_types = {
        "channel": {
            "method": Connector().get_channel_list,
            "fields": ",".join(PERSISTENT_SEGMENT_CHANNEL_PREVIEW_FIELDS),
            "sort": "channel_id",
            "channel_id__terms": related_ids
        },
        "video": {
            "method": Connector().get_video_list,
            "fields": ",".join(PERSISTENT_SEGMENT_VIDEO_PREVIEW_FIELDS),
            "sort": "video_id",
            "video_id__terms": related_ids
        }
    }
    config = valid_segment_types.get(segment_type)
    return config
