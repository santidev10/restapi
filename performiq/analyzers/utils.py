def _try_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except ValueError:
            result = None
        return result
    return wrapper


class Coercers:
    @staticmethod
    @_try_decorator
    def percentage(val):
        return float(val.strip("%"))

    @staticmethod
    @_try_decorator
    def float(val):
        return float(val)

    @staticmethod
    @_try_decorator
    def cost_micros(val):
        return float(val) / 10**6

    @staticmethod
    @_try_decorator
    def integer(val):
        return int(val)

    @staticmethod
    @_try_decorator
    def raw(val):
        return val

    @staticmethod
    @_try_decorator
    def channel_url(val):
        return val.split("/channel/")[-1].strip("/")
