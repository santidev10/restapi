from django.db.models import Q
from rest_framework.generics import GenericAPIView
from rest_framework.generics import ListCreateAPIView
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT

from channel.api.views import ChannelListApiView
from segment.api.serializers import SegmentSerializer
from segment.models import get_segment_model_by_type
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException
from utils.api_paginator import CustomPageNumberPaginator


class SegmentPaginator(CustomPageNumberPaginator):
    """
    Paginator for segments list
    """
    page_size = 10


class DynamicModelViewMixin(object):
    def dispatch(self, request, segment_type, **kwargs):
        self.model = get_segment_model_by_type(segment_type)
        self.serializer_class.Meta.model = self.model
        return super().dispatch(request, **kwargs)

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        if self.request.user.is_staff:
            queryset = self.model.objects.all()
        elif self.request.user.has_perm('userprofile.view_pre_baked_segments'):
            queryset = self.model.objects.filter(
                Q(owner=self.request.user) |
                ~Q(category='private'))
        else:
            queryset = self.model.objects.filter(owner=self.request.user)
        return queryset


class SegmentListCreateApiView(DynamicModelViewMixin, ListCreateAPIView):
    """
    Segment list/create endpoint
    """
    serializer_class = SegmentSerializer
    pagination_class = SegmentPaginator

    default_allowed_sorts = {
        "title",
        "videos",
        "engage_rate",
        "sentiment",
        "created_at",
    }
    allowed_sorts = {
        "channel": default_allowed_sorts.union({"channels"}),
        "keyword": {"competition", "average_cpc", "average_volume"}
    }

    def do_filters(self, queryset):
        """
        Filter queryset
        """
        filters = {}
        # search
        search = self.request.query_params.get("search")
        if search:
            filters["title__icontains"] = search
        # category
        category = self.request.query_params.get("category")
        if category:
            filters["category"] = category
        # make filtering
        if filters:
            queryset = queryset.filter(**filters)
        return queryset

    def do_sorts(self, queryset):
        """
        Sort queryset
        """
        segment = self.model.segment_type
        allowed_sorts = self.allowed_sorts.get(segment,
                                               self.default_allowed_sorts)

        def get_sort_prefix():
            """
            Define ascending or descending sort
            """
            reverse = "-"
            ascending = self.request.query_params.get("ascending")
            if ascending == "1":
                reverse = ""
            return reverse

        sort = self.request.query_params.get("sort_by")
        if sort in allowed_sorts:
            queryset = queryset.order_by("{}{}".format(
                get_sort_prefix(), sort))
        return queryset

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        queryset = super().get_queryset()
        queryset = self.do_filters(queryset)
        queryset = self.do_sorts(queryset)
        return queryset

    def paginate_queryset(self, queryset):
        """
        Processing flat query param
        """
        flat = self.request.query_params.get("flat")
        if flat == "1":
            return None
        return super().paginate_queryset(queryset)


class SegmentRetrieveUpdateDeleteApiView(DynamicModelViewMixin,
                                         RetrieveUpdateDestroyAPIView):
    serializer_class = SegmentSerializer

    def delete(self, request, *args, **kwargs):
        segment = self.get_object()
        user = request.user
        if not (user.is_staff or segment.owner == user):
            return Response(status=HTTP_403_FORBIDDEN)
        return super().delete(request, *args, **kwargs)

    def put(self, request, *args, **kwargs):
        """
        Allow partial update
        """
        segment = self.get_object()
        user = request.user

        if not (user.is_staff or segment.owner == user):
            return Response(status=HTTP_403_FORBIDDEN)

        serializer_context = {"request": request}

        serializer = self.serializer_class(
            instance=segment, data=request.data,
            context=serializer_context, partial=True
        )
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()

        response_data = self.serializer_class(
            segment,
            context=serializer_context
        ).data
        return Response(response_data)


class SegmentDuplicateApiView(DynamicModelViewMixin, GenericAPIView):
    serializer_class = SegmentSerializer

    def post(self, request, pk):
        """
        Make a copy of segment and attach to user
        """
        segment = self.get_object()
        duplicated_segment = segment.duplicate(request.user)

        response_data = self.serializer_class(
            duplicated_segment,
            context={"request": request}
        ).data

        return Response(response_data, status=HTTP_201_CREATED)


class SegmentSuggestedChannelApiView(DynamicModelViewMixin, GenericAPIView):
    serializer_class = SegmentSerializer
    connector = Connector()

    def get(self, request, *args, **kwargs):
        segment = self.get_object()
        query_params = self.request.query_params
        query_params._mutable = True
        response_data = []

        if segment.top_recommend_channels:
            try:
                query_params['ids'] = ','.join(
                    segment.top_recommend_channels[:100])
                response_data = self.connector.get_channel_list(query_params)
            except SingleDatabaseApiConnectorException:
                return Response(status=HTTP_408_REQUEST_TIMEOUT)
        if response_data:
            ChannelListApiView.adapt_response_data(response_data, request.user)
        return Response(response_data)
