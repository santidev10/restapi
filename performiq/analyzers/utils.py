def _try(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except (ValueError, TypeError):
            result = None
        return result
    return wrapper


class Coercers:
    @staticmethod
    @_try
    def percentage(val):
        if isinstance(val, str):
            val = float(val.strip("%"))
        else:
            val = float(val)
        return val

    @staticmethod
    @_try
    def float(val):
        return float(val)

    @staticmethod
    @_try
    def cost_micros(val):
        return float(val) / 10**6

    @staticmethod
    @_try
    def integer(val):
        return int(val)

    @staticmethod
    @_try
    def raw(val):
        return val

    @staticmethod
    @_try
    def channel_url(val):
        return val.split("/channel/")[-1].strip("/")
