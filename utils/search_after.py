from operator import attrgetter

from es_components.query_builder import QueryBuilder


def search_after(query: QueryBuilder, manager, sort=None, size=1000):
    """
    Uses search after API to retrieve results using sort cursor
    :param query: Query to retrieve documents
    :param manager: ChannelManager or VideoManager object
    :param sort: Sort
    :param size:
    :return:
    """
    sort = sort or {"main.id": {"order": "asc"}}
    data = manager.search(query, limit=size).sort(sort).execute()
    yield data

    # Set up attribute getters to get sort values to pass into search_after
    getters = [attrgetter(field) for field in sort.keys()]
    while data:
        try:
            last = [getter(data[-1]) for getter in getters]
        except (IndexError, AttributeError):
            return
        data = manager.search(query, limit=size).sort(sort).extra(search_after=last).execute()
        if not data:
            return
        yield data

