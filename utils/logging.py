import inspect
from functools import wraps


def log_all_methods(logger):
    def decorator(cls):
        log = log_method(logger, cls.__name__ + ".")
        for attr in cls.__dict__:
            if attr.startswith("__") and attr.endswith("__"):
                continue
            prop = getattr(cls, attr)
            if callable(prop):
                setattr(cls, attr, log(prop))
        return cls

    return decorator


def log_method(logger, prefix=""):
    def log_fn(fn):
        first_arg_name = (list(inspect.signature(fn).parameters) or [None])[0]
        is_class_method = first_arg_name == "cls"
        is_static_method = first_arg_name not in ("cls", "self")

        @wraps(fn)
        def decorator(*args, **kwargs):
            if is_class_method:
                args = (type(args[0])) + args[1:]
            elif is_static_method:
                args = args[1:]
            logger.debug(
                "(+) {}{} was called".format(prefix, fn.__name__))
            res = fn(*args, **kwargs)
            logger.debug(
                "(-) {}{} finished".format(prefix, fn.__name__, ))
            return res

        return decorator

    return log_fn
