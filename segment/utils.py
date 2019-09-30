import time

from segment.models.persistent.base import BasePersistentSegment
import brand_safety.constants as constants


class ModelDoesNotExist(Exception):
    pass


@property
def SEGMENT_TYPES():
    return [constants.CHANNEL, constants.VIDEO]


@property
def PERSISTENT_SEGMENT_MODELS():
    return [m for m in BasePersistentSegment.__subclasses__()]


@property
def PERSISTENT_SEGMENT_TYPES():
    return [m.segment_type for m in PERSISTENT_SEGMENT_MODELS.fget()]


def get_persistent_segment_model_by_type(segment_type):
    for model in PERSISTENT_SEGMENT_MODELS.fget():
        if model.segment_type == segment_type:
            return model
    raise ModelDoesNotExist("Invalid segment_type: %s" % segment_type)


def retry_on_conflict(method, *args, retry_amount=10, sleep_coeff=2, **kwargs):
    """
    Retry on Document Conflicts
    """
    tries_count = 0
    try:
        while tries_count <= retry_amount:
            try:
                result = method(*args, **kwargs)
            except Exception as err:
                if "ConflictError(409" in str(err):
                    tries_count += 1
                    if tries_count <= retry_amount:
                        sleep_seconds_count = retry_amount ** sleep_coeff
                        time.sleep(sleep_seconds_count)
                else:
                    raise err
            else:
                return result
    except Exception:
        raise


def generate_search_with_params(manager, query, sort=None):
    """
    Generate scan query with sorting
    :param manager:
    :param query:
    :param sort:
    :return:
    """
    search = manager._search()
    search = search.query(query)
    if sort:
        search = search.sort(sort)
    search = search.params(preserve_order=True)
    return search
