from rest_framework.generics import ListAPIView
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.response import Response
from rest_framework.views import APIView

from singledb.models import Channel

from singledb.api.pagination import ListPaginator
from singledb.api.serializers import ChannelSerializer
from singledb.api.utils import ChannelFiltersGenerator


class RetrieveUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = tuple()
    serializer_class = ChannelSerializer

    def get_queryset(self):
        queryset = Channel.objects.all().select_related("details")
        return queryset


class ListFiltersApiView(APIView):
    permission_classes = tuple()
    allowed_filters = ["countries", "categories"]

    def get(self, request):
        channel_filter = request.query_params.get("filter")

        if channel_filter not in self.allowed_filters:
            return Response( {"error": ["invalid filter"]}, HTTP_400_BAD_REQUEST)

        queryset = Channel.objects.all()
        filters_generator = ChannelFiltersGenerator(queryset)
        generator = getattr(filters_generator, "generate_{}_filter".format(channel_filter))

        return Response(data=generator())


class ListApiView(ListAPIView):
    permission_classes = tuple()
    serializer_class = ChannelSerializer
    pagination_class = ListPaginator
    allowed_sorts = ["subscribers", "sentiment", "engagement", "views_per_video", "thirty_days_views", "thirty_days_subscribers"]

    def do_sorts(self, queryset):
        sorting = self.request.query_params.get("sort_by")
        if sorting in self.allowed_sorts:
            if sorting == "engagement":
                sorting = "engage_rate"
        else:
            sorting = self.allowed_sorts[0]
        return queryset.order_by("-details__{}".format(sorting))

    def do_filters(self, queryset):
        min_subscribers = self.request.query_params.get("min_subscribers_yt")
        filters = {}
        if min_subscribers:
            filters['details__subscribers__gte'] = min_subscribers
        if filters:
            try:
                queryset = queryset.filter(**filters)
            except ValueError:
                queryset = Channel.objects.none()
        return queryset

    def get_queryset(self):
        queryset = Channel.objects.all().select_related("details")
        queryset = self.do_filters(queryset)
        queryset = self.do_sorts(queryset)
        return queryset
