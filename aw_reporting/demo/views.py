from functools import wraps

from rest_framework.response import Response
from rest_framework.status import HTTP_403_FORBIDDEN

DEMO_READ_ONLY = dict(error="You are not allowed to change this entity")


def forbidden_for_demo(is_demo):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if is_demo(*args, **kwargs):
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            return fn(*args, **kwargs)

        return wrapper

    return decorator
