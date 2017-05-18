"""
Segment api views module
"""
from django.db.models import Q
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED, HTTP_404_NOT_FOUND, \
    HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from segment.api.serializers import SegmentCreateSerializer, SegmentSerializer
from segment.models import Segment
from utils.api_paginator import CustomPageNumberPaginator
from utils.single_database_connector import SingleDatabaseApiConnector
from utils.single_database_connector import SingleDatabaseApiConnectorException


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
        response_data = self.serializer_class(
            segment, context=serializer_context).data
        return Response(response_data, status=HTTP_201_CREATED)

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        if self.request.user.is_staff:
            queryset = Segment.object.all()
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


class SegmentChannelListApiView(APIView):
    """
    Segment channels list endpoint
    """
    def get(self, request, pk):
        """
        Obtain segment channels procedure
        """
        # obtain segment
        try:
            if request.user.is_staff:
                segment = Segment.objects.get(id=pk)
            else:
                segment = Segment.objects.filter(
                    Q(owner=self.request.user) |
                    ~Q(category="private")).get(id=pk)
        except Segment.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        # obtain channels ids
        channels_ids = segment.channels.values_list("channel_id", flat=True)
        query_params = {"ids": ",".join(channels_ids)}
        # execute call to single db
        connector = SingleDatabaseApiConnector()
        try:
            response_data = connector.get_channel_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        # TODO we should add processing of potential deleted channels
        return Response(response_data)
