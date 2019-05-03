from collections import defaultdict


class MockRedisLock:
    """Poorly imitate a Redis lock object so unit tests can run without needing a real Redis server."""

    def __init__(self, redis, name, timeout=None, sleep=0.1):
        self.redis = redis
        self.name = name
        self.acquired_until = None
        self.timeout = timeout
        self.sleep = sleep

    def acquire(self, blocking=True):
        return True

    def release(self):
        return

    def do_release(self, *args, **kwargs):
        return


class MockRedis:
    """Imitate a Redis object so unit tests can run without needing a real Redis server."""

    redis = defaultdict(dict)

    def __init__(self):
        pass

    def delete(self, key):
        if key in MockRedis.redis:
            del MockRedis.redis[key]

    def get(self, key):
        result = '' if key not in MockRedis.redis else MockRedis.redis[key]
        return result

    def lock(self, key, timeout=0, sleep=0):
        return MockRedisLock(self, key)