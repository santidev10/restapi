from utils.api_paginator import CustomPageNumberPaginator
from utils.es_components_api_utils import PaginatorWithAggregationMixin
from utils.es_components_api_utils import ESQuerysetAdapter


# todo: merge with ESQuerysetAdapter
class ESRetrieveAdapter:
    def __init__(self, manager):
        self.manager = manager
        self.search_id = None
        self.fields_to_load = None

    def id(self, search_id):
        self.search_id = search_id
        return self

    # pylint: disable=all
    def fields(self, fields=()):
        fields = [
            field
            for field in fields
            if field.split(".")[0] in self.manager.sections
        ]

        self.fields_to_load = fields or self.manager.sections
        return self
    # pylint: enable=all

    def get_data(self):
        item = self.manager.model.get(self.search_id, _source=self.fields_to_load)
        return item


class ESEmptyResponseAdapter(ESQuerysetAdapter):
    def get_data(self, *args, **kwargs):
        return []

    def count(self):
        return 0


class ResearchPaginator(PaginatorWithAggregationMixin, CustomPageNumberPaginator):
    page_size = 50
    page_size_query_param = "size"
    max_page_number = 200
