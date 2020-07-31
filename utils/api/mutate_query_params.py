from contextlib import contextmanager


@contextmanager
def mutate_query_params(query_params):
    # pylint: disable=protected-access
    query_params._mutable = True
    yield query_params
    query_params._mutable = False
    # pylint: enable=protected-access


class MutateQueryParamIfValidYoutubeIdMixin:

    YOUTUBE_ID_FIELD = "main.id"

    def mutate_query_params_if_valid_youtube_id(self, manager):
        """
        modifies search if only a single search query for match_phrase_filter
        fields is present and is a valid Channel or Video id. Remove all
        match_phrase_filter fields and replace with the single `main.id`
        terms_filter field
        """
        search_values = [self.request.query_params.get(field, None) for field in self.match_phrase_filter]
        search_values = list(set(filter(None, search_values)))
        if len(search_values) == 1 and self.YOUTUBE_ID_FIELD in self.terms_filter and manager.get(ids=search_values):
            with mutate_query_params(self.request.query_params):
                self.request.query_params[self.YOUTUBE_ID_FIELD] = search_values[0]
                for field in self.match_phrase_filter:
                    if self.request.query_params.get(field, None):
                        del self.request.query_params[field]
