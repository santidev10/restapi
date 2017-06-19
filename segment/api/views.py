from django.db.models import Q
from django.db.models.expressions import RawSQL
from rest_framework.generics import GenericAPIView
from rest_framework.generics import ListCreateAPIView
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_403_FORBIDDEN

from segment.api.serializers import SegmentSerializer
from segment.models import get_segment_model_by_type
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
        else:
            queryset = self.model.objects.filter(
                Q(owner=self.request.user) |
                ~Q(category="private"))
        return queryset


class SegmentListCreateApiView(DynamicModelViewMixin, ListCreateAPIView):
    serializer_class = SegmentSerializer
    pagination_class = SegmentPaginator

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
        available_sorts = {
            "title",
        }
        available_reverse_sorts = {
            "videos",
            "engage_rate",
            "sentiment",
            "created_at",
        }
        if self.model.segment_type == 'channel':
            available_reverse_sorts.add('channels')

        sort = self.request.query_params.get("sort_by")

        if sort in available_sorts:
            queryset = queryset.order_by(sort)

        elif sort in available_reverse_sorts:
            queryset = queryset.order_by("-{}".format(sort))

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


class SegmentRetrieveUpdateDeleteApiView(DynamicModelViewMixin, RetrieveUpdateDestroyAPIView):
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
        related_manager = self.model.related.rel.related_model.objects

        related_list = list(segment.related.all())

        duplicated_segment_data = {
            "title": "{} (copy)".format(segment.title),
            "category": "private",
            "statistics": segment.statistics,
            "mini_dash_data": segment.mini_dash_data,
            "owner": request.user
        }

        duplicated_segment = self.model.objects.create(**duplicated_segment_data)
        for related in related_list:
            related.pk = None
            related.segment = duplicated_segment
        related_manager.bulk_create(related_list)

        response_data = self.serializer_class(
            duplicated_segment, context={"request": request}).data

        return Response(response_data, status=HTTP_201_CREATED)
