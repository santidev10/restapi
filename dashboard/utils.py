import hashlib


def get_cache_key(data, prefix=None):
    prefix = prefix or ""
    return str(prefix) + hashlib.sha256(str(data).encode("utf8")).hexdigest()
