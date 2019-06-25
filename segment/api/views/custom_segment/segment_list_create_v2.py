from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.generics import ListCreateAPIView

from audit_tool.models import get_hash_name
from brand_safety.utils import BrandSafetyQueryBuilder
from segment.api.serializers import CustomSegmentSerializer
from segment.api.paginator import SegmentPaginator
from segment.models.custom_segment import CustomSegment
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload


class SegmentListCreateApiViewV2(ListCreateAPIView):
    REQUIRED_FIELDS = ["brand_safety_categories", "languages", "list_type", "minimum_option", "score_threshold", "title", "youtube_categories"]
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
    queryset = CustomSegment.objects.all().order_by("id")

    def _do_filters(self, queryset):
        """
        Filter queryset
        """
        filters = {}
        is_admin = self.request.user.is_staff
        if not is_admin:
            filters["owner_id"] = self.request.user.id
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
        except ValueError as err:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=str(err)
            )
        data["owner"] = request.user.id
        data["segment_type"] = kwargs["segment_type"]
        data["title_hash"] = get_hash_name(data["title"].lower().strip())
        serializer = self.serializer_class(data=data)
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()
        data["youtube_categories"] = BrandSafetyQueryBuilder.map_youtube_categories(data["youtube_categories"])
        query_builder = BrandSafetyQueryBuilder(data)
        CustomSegmentFileUpload.enqueue(query=query_builder.query_body, segment=segment)
        return Response(status=HTTP_201_CREATED, data=serializer.data)

    def _validate_data(self, data):
        expected = set(self.REQUIRED_FIELDS)
        received = set(data.keys())
        if expected != received:
            err = "Fields must consist of: {}".format(", ".join(self.REQUIRED_FIELDS))
        else:
            err = self.validate_threshold(data["score_threshold"])
        if err:
            raise ValueError(err)

    @staticmethod
    def validate_threshold(threshold):
        err = None
        if not 0 <= threshold <= 100:
            err = "Score threshold must be between 0 and 100, inclusive."
        return err
