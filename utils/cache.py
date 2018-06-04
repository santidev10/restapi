from hashlib import md5

from django.conf import settings
from django.core.cache import cache
from rest_framework.response import Response

EMPTY_REGISTRY = {'history': [], 'keys': {}}


def registry(key, value=None):
    _registry = cache.get(settings.CACHE_MAIN_KEY, EMPTY_REGISTRY)
    if value is not None:
        _registry[key] = value
        cache.set(settings.CACHE_MAIN_KEY, _registry)
    return _registry[key]


def rated_pages():
    history = registry('history')
    pages = {}
    for path in history:
        if path not in pages:
            pages[path] = 0
        pages[path] += 1

    return sorted(pages.items(),
                  key=lambda x: -x[1])[:settings.CACHE_PAGES_LIMIT]


def cache_reset():
    keys = registry('keys')
    for key in keys.keys():
        cache.delete(key)
    registry('keys', {})


def cached_view_decorator(method):
    if not settings.CACHE_ENABLED:
        return method

    def wrapped_get(self, request, *args, **kwargs):
        update_cache_only = bool('HTTP_X_CACHE_UPDATE' in request.META
                                 and request.META['HTTP_X_CACHE_UPDATE'])

        path = request.get_full_path()
        if request.user and request.user.is_authenticated() \
                and not request.user.is_superuser:
            path = "{}{}".format(path, "influencer")
        key = settings.CACHE_KEY_PREFIX + md5(path.encode()).hexdigest()

        # get/set cached response
        cached_response = cache.get(key, None) \
            if not update_cache_only \
            else None
        skip_caching = False
        if cached_response:
            response = Response(**cached_response)
            response['X-Cached-Content'] = True
        else:
            response = method(self=self, request=request, *args, **kwargs)
            if isinstance(response, Response) and response.status_code < 300:
                response['X-Cached-Content'] = False
                cached_response = {'data': response.data,
                                   'status': response.status_code}
                cache.set(key, cached_response, settings.CACHE_TIMEOUT)
            else:
                skip_caching = True

        if not skip_caching:
            # update requests history
            if not update_cache_only:
                history = registry('history')
                history.append(request.get_full_path())
                history_length = len(history)
                if history_length > settings.CACHE_HISTORY_LIMIT:
                    history = history[
                              history_length - settings.CACHE_HISTORY_LIMIT:]
                registry('history', history)

            # update keys in registry
            keys = registry('keys')
            keys[key] = request.get_full_path()
            keys_to_delete = [k for k in keys.keys() if not cache.get(k, None)]
            for k in keys_to_delete:
                del keys[k]
            registry('keys', keys)

        return response

    def wrapped_put_update(self, *args, **kwargs):
        cache_reset()
        return method(self, *args, **kwargs)

    wrapped = {'get': wrapped_get,
               'get_for_exportable': wrapped_get,
               'put': wrapped_put_update,
               'post': wrapped_put_update,
               'delete': wrapped_put_update}

    assert method.__name__ in wrapped, \
        "Unsupported method name: '{}'".format(method.__name__)
    return wrapped[method.__name__]
