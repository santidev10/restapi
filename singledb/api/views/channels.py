from django.db.models import Q
from rest_framework.generics import ListAPIView
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.response import Response
from rest_framework.views import APIView

from singledb.models import Channel

from singledb.api.pagination import ListPaginator
from singledb.api.serializers import ChannelDetailsSerializer
from singledb.api.serializers import ChannelListSerializer
from singledb.api.utils import ChannelFiltersGenerator


class ChannelRetrieveUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = tuple()
    serializer_class = ChannelDetailsSerializer

    def get_queryset(self):
        queryset = Channel.objects.all().select_related("details")
        return queryset


class ChannelListFiltersApiView(APIView):
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


class ChannelListApiView(ListAPIView):
    permission_classes = tuple()
    serializer_class = ChannelListSerializer
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
        filters = {}
        exclude = {}

        # --- Filter groups ---
        filter_group = self.request.query_params.get("filter_group")
        if filter_group:
            if filter_group == 'influencer':
                filters['details__subscribers__gte'] = 10000
                filters['details__subscribers__lte'] = 100000
            elif filter_group == 'brands':
                #TODO: implement filter or brands
                pass
            elif filter_group == 'entertainment':
                filters['ptk_value__in'] = ['vevo']
            elif filter_group == 'all':
                pass
            else:
                queryset = Channel.objects.none()

        # --- Filters ---
        # selected ids
        selected_ids = self.request.query_params.get("ids")
        if selected_ids:
            selected_ids = selected_ids.split(",")
            filters["id__in"] = selected_ids
        # country
        country = self.request.query_params.get("country")
        if country:
            filters["country__in"] = country.split(",")
        # email
        email = self.request.query_params.get("email")
        if email == "1":
            exclude["emails"] = ""
        if email == "0":
            filters["emails"] = ""
        # min_subscribers
        min_subscribers = self.request.query_params.get("min_subscribers_yt")
        if min_subscribers:
            filters['details__subscribers__gte'] = min_subscribers
        # max_subscribers
        max_subscribers = self.request.query_params.get("max_subscribers_yt")
        if max_subscribers:
            filters['details__subscribers__lte'] = max_subscribers
        # preferred
        preferred = self.request.query_params.get("preferred")
        if preferred:
            filters['preferred'] = int(preferred)

        if filters:
            try:
                queryset = queryset.filter(**filters)
            except ValueError:
                queryset = Channel.objects.none()
        if exclude:
            try:
                queryset = queryset.exclude(**exclude)
            except ValueError:
                queryset = Channel.objects.none()

        # category
        category = self.request.query_params.get("category")
        if category:
            categories = category.split(",")
            category_search = reduce(operator.or_, (Q(category__icontains=x) for x in categories))
            queryset = queryset.filter(category_search)

        return queryset


    def get_queryset(self):
        queryset = Channel.objects.all().select_related("details")
        queryset = self.do_filters(queryset)
        queryset = self.do_sorts(queryset)
        return queryset
