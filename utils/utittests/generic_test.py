import logging

from django.db import transaction

logger = logging.getLogger(__name__)


def generic_test(args_list, debug_indexes=None):
    """
    Generates subtest per each item in the args_list
    :param args_list: (msg: str, args: List, kwargs: Dict)
    :param debug_indexes:
    :return:
    """
    if debug_indexes:
        logger.warning("Do not commit with `debug_indexes`. For debug purposes only")
        args_list = [args_list[index] for index in debug_indexes]

    def wrapper(fn):
        def wrapped_test_function(self):
            for msg, args, kwargs in args_list:
                if msg is None:
                    msg = ", ".join([str(item) for item in args + (kwargs,)])
                with self.subTest(msg=msg or str(args), **kwargs), transaction.atomic():
                    sid = transaction.savepoint()
                    fn(self, *args, **kwargs)
                    transaction.savepoint_rollback(sid)

        return wrapped_test_function

    return wrapper
