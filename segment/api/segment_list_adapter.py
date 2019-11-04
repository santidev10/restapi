from django.http import Http404
from rest_framework.exceptions import ValidationError
from rest_framework.generics import ListAPIView
from rest_framework.status import HTTP_400_BAD_REQUEST

import brand_safety.constants as constants
from channel.api.serializers.channel_with_blacklist_data import ChannelWithBlackListSerializer
from es_components.constants import Sections
from video.api.serializers.video_with_blacklist_data import VideoWithBlackListSerializer
from utils.api_paginator import CustomPageNumberPaginator
from utils.es_components_api_utils import ESQuerysetAdapter


class SegmentListAPIViewAdapter(ListAPIView):
    """
    View to provide preview data for persistent segments
    """
    MAX_PAGE_SIZE = 10
    DEFAULT_PAGE_SIZE = 5
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)
    pagination_class = CustomPageNumberPaginator

    def get_serializer_class(self):
        segment_type = self.kwargs["segment_type"]
        if segment_type == constants.CHANNEL:
            serializer = ChannelWithBlackListSerializer
        else:
            serializer = VideoWithBlackListSerializer
        return serializer

    def get_queryset(self):
        pk = self.kwargs["pk"]
        try:
            segment = self.segment_model.objects.get(id=pk)
        except self.segment_model.DoesNotExist:
            raise Http404
        self.request.query_params._mutable = True
        page = self.request.query_params.get("page", 0)
        size = self.request.query_params.get("size", 0)
        try:
            page = int(page)
        except ValueError:
            raise ValidationError(code=HTTP_400_BAD_REQUEST, detail=f"Invalid page number: {page}")
        try:
            size = int(size)
        except ValueError:
            raise ValidationError(code=HTTP_400_BAD_REQUEST, detail=f"Invalid page size: {size}")
        if page <= 0:
            page = 1
        if size <= 0:
            size = self.DEFAULT_PAGE_SIZE
        if size > self.MAX_PAGE_SIZE:
            size = self.MAX_PAGE_SIZE
        self.request.query_params["page"] = page
        self.request.query_params["size"] = size

        result = ESQuerysetAdapter(segment.es_manager)
        result.sort = [segment.SORT_KEY]
        result.filter([segment.get_segment_items_query()])
        return result