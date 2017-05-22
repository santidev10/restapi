"""
Segment api views module
"""
from django.db.models import Q
from rest_framework.generics import ListCreateAPIView, \
    RetrieveUpdateDestroyAPIView, GenericAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED

from segment.api.serializers import SegmentCreateSerializer, SegmentSerializer, \
    SegmentUpdateSerializer
from segment.models import Segment, ChannelRelation, VideoRelation
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
        segment.count_statistics_fields.delay(segment)
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
    update_serializer_class = SegmentUpdateSerializer

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
        segment = self.get_object()
        serializer_context = {"request": request}
        serializer = self.update_serializer_class(
            instance=segment, data=request.data,
            context=serializer_context, partial=True)
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()
        self.update_segment_relations(segment)
        segment.count_statistics_fields.delay(segment)
        response_data = self.serializer_class(
            segment, context=serializer_context).data
        return Response(response_data)

    def update_segment_relations(self, segment):
        """
        Check for dropped / added channels/videos
        """
        if segment.segment_type == "channel":
            # add channels
            channels_to_add_ids = self.request.data.get(
                "channels_to_add") or []
            channels_to_add = []
            for channel_id in channels_to_add_ids:
                obj, is_created = ChannelRelation.objects.get_or_create(
                    channel_id=channel_id)
                channels_to_add.append(obj)
            segment.channels.add(*channels_to_add)
            # remove channels
            channels_to_delete_ids = self.request.data.get(
                "channels_to_delete") or []
            segment.channels.remove(*ChannelRelation.objects.filter(
                id__in=channels_to_delete_ids))
            return
        elif segment.segment_type == "video":
            # add videos
            videos_to_add_ids = self.request.data.get(
                "videos_to_add") or []
            videos_to_add = []
            for video_id in videos_to_add_ids:
                obj, is_created = VideoRelation.objects.get_or_create(
                    video_id=video_id)
                videos_to_add.append(obj)
            segment.videos.add(*videos_to_add)
            # remove videos
            videos_to_delete_ids = self.request.data.get(
                "videos_to_delete") or []
            segment.videos.remove(*VideoRelation.objects.filter(
                id__in=videos_to_delete_ids))
            return
        return


class SegmentDuplicateApiView(GenericAPIView):
    """
    Endpoint for segment duplicate
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

    def post(self, request, pk):
        """
        Make a copy of segment and attach to user
        """
        segment = self.get_object()
        duplicated_segment_data = {
            "title": "{} (copy)".format(segment.title),
            "segment_type": segment.segment_type,
            "category": "private",
            "statistics": segment.statistics,
            "mini_dash_data": segment.mini_dash_data,
            "owner": request.user
        }
        duplicated_segment = Segment.objects.create(**duplicated_segment_data)
        duplicated_segment.channels.add(*segment.channels.all())
        duplicated_segment.videos.add(*segment.videos.all())
        response_data = self.serializer_class(
            duplicated_segment, context={"request": request}).data
        return Response(response_data, status=HTTP_201_CREATED)
