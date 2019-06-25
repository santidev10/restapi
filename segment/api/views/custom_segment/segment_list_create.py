from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from segment.api.mixins import DynamicModelViewMixin
from segment.api.paginator import SegmentPaginator
from segment.api.serializers import SegmentSerializer


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

    def __validate_filters(self):
        owner_id = self.request.query_params.get("owner_id")
        if owner_id is not None:
            return owner_id == str(self.request.user.id)\
                   or self.request.user.is_staff
        return True

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
        owner_id = self.request.query_params.get("owner_id")
        if owner_id:
            filters["owner__id"] = owner_id
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

    def get(self, request, *args, **kwargs):
        if not self.__validate_filters():
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data={"error": "invalid filter(s)"})
        return super(SegmentListCreateApiView, self).get(
            request, *args, **kwargs)
