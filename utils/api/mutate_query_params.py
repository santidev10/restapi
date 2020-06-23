from contextlib import contextmanager


@contextmanager
def mutate_query_params(query_params):
    # pylint: disable=protected-access
    query_params._mutable = True
    yield query_params
    query_params._mutable = False
    # pylint: enable=protected-access