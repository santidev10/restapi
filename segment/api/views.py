"""
Segment api views module
"""
from django.db.models import Q
from rest_framework.generics import ListCreateAPIView, \
    RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED, HTTP_405_METHOD_NOT_ALLOWED

from segment.api.serializers import SegmentCreateSerializer, SegmentSerializer
from segment.models import Segment
from utils.api_paginator import CustomPageNumberPaginator


class SegmentPaginator(CustomPageNumberPaginator):
    """
    Paginator for segments list
    """
    page_size = 10


class SegmentListCreateApiView(ListCreateAPIView):
    """
    Segment list endpoint
    """
    serializer_class = SegmentSerializer
    create_serializer_class = SegmentCreateSerializer
    pagination_class = SegmentPaginator

    def post(self, request, *args, **kwargs):
        """
        Extend post functionality
        """
        serializer_context = {"request": request}
        serializer = self.create_serializer_class(
            data=request.data, context=serializer_context)
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()
        # TODO enable after celery setup on staging
        segment.count_statistics_fields()
        # segment.count_statistics_fields.delay(segment)
        response_data = self.serializer_class(
            segment, context=serializer_context).data
        return Response(response_data, status=HTTP_201_CREATED)

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        if self.request.user.is_staff:
            queryset = Segment.objects.all()
        else:
            queryset = Segment.objects.filter(
                Q(owner=self.request.user) |
                ~Q(category="private"))
        filters = {}
        # segment type
        segment_type = self.request.query_params.get("segment_type")
        if segment_type:
            filters["segment_type"] = segment_type
        # category
        category = self.request.query_params.get("category")
        if category:
            filters["category"] = category
        return queryset.filter(**filters)


class SegmentRetrieveUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    """
    Retrieve / update / delete segment endpoint
    """
    serializer_class = SegmentSerializer

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        if self.request.user.is_staff:
            queryset = Segment.objects.all()
        else:
            queryset = Segment.objects.filter(
                Q(owner=self.request.user) |
                ~Q(category="private"))
        return queryset

    def put(self, request, *args, **kwargs):
        """
        Allow partial update
        """
        # TODO discuss the algorithm
        return Response(status=HTTP_405_METHOD_NOT_ALLOWED)
