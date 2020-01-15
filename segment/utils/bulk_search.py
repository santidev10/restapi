from operator import attrgetter

from elasticsearch_dsl import Q

from es_components.query_builder import QueryBuilder


def bulk_search(model, query, sort, cursor_field, batch_size=10000, source=None, options=None, direction=0):
    """
    Util function to retrieve items greater than Elasticsearch limit by using cursor
    :param model: Elasticsearch model
    :param query: Base query
    :param sort: list
    :param cursor_field: str -> Field to use as cursor when retrieving items
    :param options: list -> Additional queries to sequentially apply to base query
        This is to ensure retrieving items with specific filters in order
        # First retrieve all items with monetization, then all items without monetization
        options = [
            QueryBuilder().build().must().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(True).get(),
            QueryBuilder().build().must_not().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(True).get(),
        ]
    :param batch_size: int
    :param source: list[str] -> Returned document fields to deserialize
    :param direction:
        0 -> Retrieve items descending
        1 -> Retrieve items ascending
    :return:
    """
    base_search = model.search().sort(*sort)
    if source:
        base_search = base_search.source(source)
    # If no options set, use base query
    if options is None:
        options = [None]
    # Create generator for each option to yield all results from
    generators = [
        search_generator(base_search, query, cursor_field, size=batch_size, option=option, direction=direction)
        for option in options
    ]
    # Yield all results from each generator sequentially
    for gen in generators:
        yield from gen


def search_generator(search, query, cursor_field, size=1000, option=None, direction=0):
    """
    Helper function to encapsulate batch cursor queries
        Search object should have all options except query applied
        Max / min item obtained using query and sort applied to search object and used as initial cursor
        Direction determines subsequent gte or lte range queries
        First query uses gte / lte option to include min/max item in first batch
        Subsequent queries will use gt / lt to exclude last item retrieved in each batch
    :param search: Elasticsearch Search object
    :param query: QueryBuilder object
    :param cursor_field: str
    :param size: int
    :param option: QueryBuilder object
    :param direction: int
    :return:
    """
    try:
        search.query = query
        first = search.execute().hits[0]
        cursor = attrgetter(cursor_field)(first)
    except IndexError:
        return
    if type(query) is dict:
        query = Q(query)
    if direction != 0 and direction != 1:
        raise ValueError(f"direction kwarg must be 0 or 1")

    if direction == 0:
        range_opt = "lte"
    else:
        range_opt = "gte"
    while True:
        cursor_query = QueryBuilder().build().must().range().field(cursor_field)
        cursor_query = getattr(cursor_query, range_opt)(cursor).get()
        full_query = query & cursor_query & option if option is not None else query & cursor_query
        search.query = full_query
        results = search[0:size].execute()
        data = results.hits
        if data:
            yield data
            cursor = attrgetter(cursor_field)(data[-1])
            # first has been processed, change queries to lt / gt
            range_opt = range_opt[:-1] if first is not None else range_opt
            first = None
        else:
            break