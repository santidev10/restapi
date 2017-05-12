from rest_framework.generics import ListAPIView
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.response import Response
from rest_framework.views import APIView

from singledb.models import Video
from singledb.models import TRENDINGS
from singledb.models import get_trending_model_by_name

from singledb.api.pagination import ListPaginator
from singledb.api.serializers import VideoSerializer
from singledb.api.utils import VideoFiltersGenerator


class RetrieveUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = tuple()
    serializer_class = VideoSerializer

    def get_queryset(self):
        queryset = Video.objects.all().select_related('channel', 'channel__details')
        return queryset


class ListFiltersApiView(APIView):
    permission_classes = tuple()
    allowed_filters = ["channels", "languages", "statuses", "categories", "countries", "content_owners"]

    def prepare_queryset(self):
        trending = self.request.query_params.get("trending")
        if trending in TRENDINGS.fget():
            model = get_trending_model_by_name(trending)
            queryset = model.objects.filter(rate__gt=0)
            video_ids = list(set(queryset.values_list('video', flat=True)))
            queryset = Video.objects.filter(id__in=video_ids)
        else:
            queryset = Video.objects.all()
        return queryset

    def get(self, request):
        error_filter_response = Response({"error": ["invalid filter"]}, HTTP_400_BAD_REQUEST)
        video_filter = request.query_params.get('filter')
        if video_filter not in self.allowed_filters:
            return Response( {"error": ["invalid filter"]}, HTTP_400_BAD_REQUEST)
        queryset = self.prepare_queryset()
        filters_generator = VideoFiltersGenerator(queryset)
        requested_filter = getattr(filters_generator, "generate_{}_filter".format(video_filter))
        return Response(data=requested_filter())


class ListApiView(ListAPIView):
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
