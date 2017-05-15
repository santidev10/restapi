from dateutil.parser import parse
from datetime import timedelta
from django.utils import timezone
from rest_framework.generics import ListAPIView
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.response import Response
from rest_framework.views import APIView

from singledb.models import Video
from singledb.models import TRENDINGS
from singledb.models import get_trending_model_by_name
from singledb.api.pagination import ListPaginator
from singledb.api.serializers import VideoDetailsSerializer
from singledb.api.serializers import VideoListSerializer
from singledb.api.utils import VideoFiltersGenerator


class VideoRetrieveUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = tuple()
    serializer_class = VideoDetailsSerializer

    def get_queryset(self):
        queryset = Video.objects.all().select_related('channel', 'channel__details')
        return queryset


class VideoListFiltersApiView(APIView):
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


class VideoListApiView(ListAPIView):
    permission_classes = tuple()
    serializer_class = VideoListSerializer
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
        exclude = {}

        # selected ids
        selected_ids = self.request.query_params.get("ids")
        if selected_ids:
            selected_ids = selected_ids.split(",")
            filters["id__in"] = selected_ids
        # channel
        channel_id = self.request.query_params.get("channel")
        if channel_id:
            channels_ids = channel_id.split(",")
            filters["channel_id__in"] = channels_ids
        # country
        country = self.request.query_params.get("country")
        if country:
            countries = country.split(",")
            filters["country__in"] = countries
        # catregory
        category = self.request.query_params.get("category")
        if category:
            categories = category.split(",")
            filters["category__in"] = categories
        # min_subscribers
        min_subscribers = self.request.query_params.get("min_subscribers")
        if min_subscribers is not None:
            try:
                min_subscribers = int(min_subscribers)
            except (TypeError, ValueError):
                return empty_queryset, True
            filters['channel__details__subscribers__gte'] = min_subscribers
        # max_subscribers
        max_subscribers = self.request.query_params.get("max_subscribers")
        if max_subscribers is not None:
            try:
                max_subscribers = int(max_subscribers)
            except (TypeError, ValueError):
                return empty_queryset, True
            filters['channel__details__subscribers__lte'] = max_subscribers
        # language
        language = self.request.query_params.get("language")
        if language:
            filters["lang_code"] = language
        # upload_at
        upload_at = self.request.query_params.get("upload_at")
        if upload_at and upload_at != "0":
            try:
                date = parse(upload_at).date()
            except (TypeError, ValueError):
                return Video.objects.none()
            filters["youtube_published_at__gte"] = date
        elif upload_at == "0":
            now = timezone.now()
            start = now - timedelta(
                hours=now.hour, minutes=now.minute,
                seconds=now.second, microseconds=now.microsecond)
            end = start + timedelta(hours=23, minutes=59,
                                    seconds=59, microseconds=999999)
            filters["youtube_published_at__range"] = (start, end)
        # min_views
        min_views = self.request.query_params.get("min_views")
        if min_views:
            filters["views__gte"] = min_views
        # max_views
        max_views = self.request.query_params.get("max_views")
        if max_views:
            filters["views__lte"] = max_views
        # min_daily_views
        min_daily_views = self.request.query_params.get("min_daily_views")
        if min_daily_views:
            filters["daily_views__gte"] = min_daily_views
        # max_daily_views
        max_daily_views = self.request.query_params.get("max_daily_views")
        if max_daily_views:
            filters["daily_views__lte"] = max_daily_views

        if filters:
            try:
                queryset = queryset.filter(**filters)
            except ValueError:
                queryset = Video.objects.none()
        if exclude:
            try:
                queryset = queryset.exclude(**exclude)
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
