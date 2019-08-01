from django.conf import settings

import brand_safety.constants as constants

from utils.es_components_api_utils import ESQuerysetAdapter
from utils.api_paginator import CustomPageNumberPaginator
from utils.es_components_api_utils import PaginatorWithAggregationMixin
from utils.es_components_api_utils import ESFilterBackend
from userprofile.permissions import PermissionGroupNames
from utils.brand_safety_view_decorator import add_brand_safety


class ESQuerysetResearchAdapter(ESQuerysetAdapter):

    def __init__(self, *args, **kwargs):
        super(ESQuerysetResearchAdapter, self).__init__(*args, **kwargs)
        self.brand_safety_index = None

    def count(self):
        count = self.manager.search(filters=self.filter_query).count()
        return count

    def brand_safety(self, brand_safety_index):
        self.brand_safety_index = brand_safety_index
        return self

    @property
    def add_extra_fields_func(self):
        return None

    def get_data(self, start=0, end=None):
        items = super(ESQuerysetResearchAdapter, self).get_data(start, end)
        if self.brand_safety_index:
            items = add_brand_safety(items, self.brand_safety_index)
        if self.add_extra_fields_func:
            for func in self.add_extra_fields_func:
                items = [func(item) for item in items]
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
        view_name = view.__class__.__name__
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


class ResearchPaginator(PaginatorWithAggregationMixin, CustomPageNumberPaginator):
    page_size = 50
    page_size_query_param = "size"