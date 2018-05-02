import inspect
from datetime import datetime
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


def log_method(logger, prefix="", fix_context=True):
    def log_fn(fn):
        first_arg_name = (list(inspect.signature(fn).parameters) or [None])[0]
        is_class_method = first_arg_name == "cls"
        is_static_method = first_arg_name not in ("cls", "self")

        @wraps(fn)
        def decorator(*args, **kwargs):
            if fix_context:
                if is_class_method:
                    args = (type(args[0])) + args[1:]
                elif is_static_method:
                    args = args[1:]
            start = datetime.now()
            res = fn(*args, **kwargs)
            end = datetime.now()
            duration = (end - start).total_seconds()
            logger.debug(
                "({}) {}{}".format(duration, prefix, fn.__name__))
            return res

        return decorator

    return log_fn


def log_function(logger, prefix=""):
    return log_method(logger, prefix=prefix, fix_context=False)
