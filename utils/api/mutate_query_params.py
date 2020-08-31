from contextlib import contextmanager


@contextmanager
def mutate_query_params(query_params):
    # pylint: disable=protected-access
    query_params._mutable = True
    yield query_params
    query_params._mutable = False
    # pylint: enable=protected-access


class AddFieldsMixin:
    ADDITIONAL_FIELDS = [
        "task_us_data.brand_safety",
        "task_us_data.last_vetted_at",
    ]

    def add_fields(self):
        fields_str = self.request.query_params.get('fields', None)
        if fields_str:
            fields = fields_str.split(',')
            for additional_field in self.ADDITIONAL_FIELDS:
                fields.append(additional_field)

            with mutate_query_params(self.request.query_params):
                self.request.query_params['fields'] = ','.join(list(set(fields)))


class MutateMappedFieldsMixin:
    """
    mutates items in the `fields` query param list. Adds a list of fields
    if a key is detected in the fields list.
    Optionally removes the keyed field.
    """

    MUTATE_FIELDS_REMOVE_KEY = 'remove'
    MUTATE_FIELDS_ADD_KEY = 'add'
    MUTATE_FIELDS_MAP = {
        'vetted_status': {
            MUTATE_FIELDS_REMOVE_KEY: ['vetted_status',],
            MUTATE_FIELDS_ADD_KEY: ['task_us_data.brand_safety',],
        }
    }

    def mutate_mapped_fields(self):
        fields_str = self.request.query_params.get('fields', None)
        if fields_str:
            fields = fields_str.split(',')
            for field in fields:
                if field in self.MUTATE_FIELDS_MAP.keys():
                    field_data = self.MUTATE_FIELDS_MAP.get(field, {})
                    fields_to_remove = field_data.get(self.MUTATE_FIELDS_REMOVE_KEY, [])
                    if fields_to_remove:
                        for field_to_remove in fields_to_remove:
                            fields.remove(field_to_remove)
                    fields_to_add = field_data.get(self.MUTATE_FIELDS_ADD_KEY, [])
                    for field_to_add in fields_to_add:
                        fields.append(field_to_add)

            with mutate_query_params(self.request.query_params):
                self.request.query_params['fields'] = ','.join(fields)


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
        if len(search_values) == 1 \
                and self.YOUTUBE_ID_FIELD in self.terms_filter \
                and list(filter(None, manager.get(ids=search_values))):
            with mutate_query_params(self.request.query_params):
                self.request.query_params[self.YOUTUBE_ID_FIELD] = search_values[0]
                for field in self.match_phrase_filter:
                    if self.request.query_params.get(field, None):
                        del self.request.query_params[field]
