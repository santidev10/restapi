import functools

from segment.models import SegmentAction


def segment_action(*action_types):
    """ Decorator for view methods that should create segment actions """
    def wrapper(method):
        @functools.wraps(method)
        def wrapped(*args, **kwargs):
            response = method(*args, **kwargs)
            try:
                if response.status_code < 300:
                    user = args[1].user
                    SegmentAction.add(user, *action_types)
            except (AttributeError, KeyError):
                pass
            return response
        return wrapped
    return wrapper
