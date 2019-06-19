from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.generics import ListCreateAPIView

from brand_safety.utils import BrandSafetyQueryBuilder
from segment.api.serializers import CustomSegmentSerializer
from segment.api.paginator import SegmentPaginator
from segment.models.custom_segment import CustomSegment
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload


class SegmentListCreateApiViewV2(ListCreateAPIView):
    REQUIRED_FIELDS = ["brand_safety_categories", "languages", "list_type", "score_threshold", "title", "youtube_categories"]
    DEFAULT_ALLOWED_SORTS = {
        "title",
        "videos",
        "engage_rate",
        "sentiment",
        "created_at",
    }
    allowed_sorts = {
        "channel": DEFAULT_ALLOWED_SORTS.union({"channels"}),
        "keyword": {"competition", "average_cpc", "average_volume"}
    }
    serializer_class = CustomSegmentSerializer
    pagination_class = SegmentPaginator
    queryset = CustomSegment.objects.all()

    def _do_filters(self, queryset):
        """
        Filter queryset
        """
        filters = {}
        # search
        search = self.request.query_params.get("search")
        if search:
            filters["title__icontains"] = search
        # category
        segment_type = self.request.query_params.get("segment_type")
        if segment_type:
            filters["segment_type"] = segment_type
        # list type
        list_type = self.request.query_params.get("list_type")
        if list_type:
            filter["list_type"] = list_type
        if filters:
            queryset = queryset.filter(**filters)
        return queryset

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        queryset = super().get_queryset()
        queryset = self._do_filters(queryset)
        return queryset

    def paginate_queryset(self, queryset):
        """
        Processing flat query param
        """
        flat = self.request.query_params.get("flat")
        if flat == "1":
            return None
        return super().paginate_queryset(queryset)

    def post(self, request, *args, **kwargs):
        data = request.data
        try:
            self._validate_data(data)
        except ValueError:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="You must provide the following fields: {}".format(", ".join(self.REQUIRED_FIELDS))
            )
        data.update(kwargs)
        serializer = self.serializer_class(data=data)
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()
        query_builder = BrandSafetyQueryBuilder(data)
        export = CustomSegmentFileUpload.enqueue(query=query_builder.query_body, segment=segment)
        data = {
            "segment_id": segment.id,
            "export_id": export.id
        }
        return Response(status=HTTP_201_CREATED, data=data)

    def _validate_data(self, data, *args, **kwargs):
        expected = set(self.REQUIRED_FIELDS)
        received = set(data.keys())
        if expected != received:
            raise ValueError
