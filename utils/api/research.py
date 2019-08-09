from django.conf import settings
from rest_framework.generics import RetrieveAPIView

import brand_safety.constants as constants
from userprofile.permissions import PermissionGroupNames
from utils.api_paginator import CustomPageNumberPaginator
from utils.brand_safety_view_decorator import add_brand_safety
from utils.es_components_api_utils import ESDictSerializer
from utils.es_components_api_utils import ESFilterBackend
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_api_utils import PaginatorWithAggregationMixin


class ESRetrieveAdapter:
    def __init__(self, manager):
        self.manager = manager
        self.search_id = None
        self.fields_to_load = None
        self.add_extra_fields_func = None
        self.brand_safety_index = None

    def extra_fields_func(self, func):
        self.add_extra_fields_func = func
        return self

    def brand_safety(self, brand_safety_index):
        self.brand_safety_index = brand_safety_index
        return self

    def id(self, search_id):
        self.search_id = search_id
        return self

    def fields(self, fields=()):
        fields = [
            field
            for field in fields
            if field.split(".")[0] in self.manager.sections
        ]

        self.fields_to_load = fields or self.manager.sections
        return self

    def get_data(self):
        item = self.manager.model.get(self.search_id, _source=self.fields_to_load)
        if self.brand_safety_index:
            item = add_brand_safety([item], self.brand_safety_index)[0]
        if self.add_extra_fields_func:
            for func in self.add_extra_fields_func:
                item = func([item])[0]
        return item


class ESQuerysetWithBrandSafetyAdapter(ESQuerysetAdapter):

    def __init__(self, *args, **kwargs):
        super(ESQuerysetWithBrandSafetyAdapter, self).__init__(*args, **kwargs)
        self.brand_safety_index = None
        self.add_extra_fields_func = None

    def brand_safety(self, brand_safety_index):
        self.brand_safety_index = brand_safety_index
        return self

    def extra_fields_func(self, func):
        self.add_extra_fields_func = func
        return self

    def get_data(self, start=0, end=None):
        items = super(ESQuerysetWithBrandSafetyAdapter, self).get_data(start, end)
        if self.brand_safety_index:
            items = add_brand_safety(items, self.brand_safety_index)
        if self.add_extra_fields_func:
            for func in self.add_extra_fields_func:
                items = func(items)
        return items


class ESBrandSafetyFilterBackend(ESFilterBackend):
    def _get_brand_safety_options(self, request, view):
        view_name = view.__class__.__name__
        if view_name not in constants.BRAND_SAFETY_DECORATED_VIEWS:
            return False
        if not request.user.groups.filter(name=PermissionGroupNames.BRAND_SAFETY_SCORING).exists():
            return False
        return True

    def _get_brand_safety_index_name(self, view):
        view_name = view.__class__.__name__.lower()
        if constants.CHANNEL in view_name:
            return settings.BRAND_SAFETY_CHANNEL_INDEX
        elif constants.VIDEO in view_name:
            return settings.BRAND_SAFETY_VIDEO_INDEX

    def filter_queryset(self, request, queryset, view):
        _filter_queryset = super(ESBrandSafetyFilterBackend, self).filter_queryset(request, queryset, view)
        brand_safety_index = None
        if self._get_brand_safety_options(request, view):
            brand_safety_index = self._get_brand_safety_index_name(view)
        return _filter_queryset.brand_safety(brand_safety_index)


class ESRetrieveApiView(RetrieveAPIView):
    serializer_class = ESDictSerializer


class ResearchPaginator(PaginatorWithAggregationMixin, CustomPageNumberPaginator):
    page_size = 50
    page_size_query_param = "size"
    max_page_number = 200
