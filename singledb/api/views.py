from rest_framework.generics import ListAPIView
from rest_framework.generics import RetrieveUpdateDestroyAPIView

from singledb.models import Channel
from singledb.models import Video
from singledb.models import TRENDINGS
from singledb.models import get_trending_model_by_name

from singledb.api.pagination import ListPaginator
from singledb.api.serializers import ChannelSerializer
from singledb.api.serializers import VideoSerializer


class ChannelListApiView(ListAPIView):
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


class ChannelRetrieveUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = tuple()
    serializer_class = ChannelSerializer

    def get_queryset(self):
        queryset = Channel.objects.all().select_related("details")
        return queryset


class VideoListApiView(ListAPIView):
    permission_classes = tuple()
    serializer_class = VideoSerializer
    pagination_class = ListPaginator
    allowed_sorts = ["views", "likes", "dislikes", "comments", "sentiment", "engagement"]

    def do_sorts(self, queryset):
        sorting = self.request.query_params.get("sort_by")
        if sorting in self.allowed_sorts:
            if sorting == "engagement":
                sorting = "engage_rate"
        else:
            sorting = self.allowed_sorts[0]
        return queryset.order_by("-{}".format(sorting))

    def do_filters(self, queryset):
        filters = {}

        channel_id = self.request.query_params.get("channel")
        if channel_id:
            channels_ids = channel_id.split(",")
            filters["channel_id__in"] = channels_ids

        if filters:
            try:
                queryset = queryset.filter(**filters)
            except ValueError:
                queryset = Video.objects.none()
        return queryset

    def get_queryset(self, export=False):
        trending = self.request.query_params.get("trending")
        if trending in TRENDINGS.fget():
            model = get_trending_model_by_name(trending)
            queryset = model.objects.filter(rate__gt=0)
            queryset = queryset.filter(segment_id__isnull=True)
            video_ids = list(set(queryset.values_list('video', flat=True)))
            queryset = Video.objects.filter(id__in=video_ids)
        else:
            queryset = Video.objects.all()
        queryset = queryset.select_related('channel', 'channel__details')
        queryset = self.do_filters(queryset)
        queryset = self.do_sorts(queryset)
        return queryset


class VideoRetrieveUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = tuple()
    serializer_class = VideoSerializer

    def get_queryset(self):
        queryset = Video.objects.all().select_related('channel', 'channel__details')
        return queryset
