from django.db.models import Count
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST

from audit_tool.models import get_hash_name
from brand_safety.utils import BrandSafetyQueryBuilder
from segment.api.serializers import CustomSegmentSerializer
from segment.api.paginator import SegmentPaginator
from segment.models.custom_segment import CustomSegment
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload


class SegmentListCreateApiViewV2(ListCreateAPIView):
    REQUIRED_FIELDS = ["brand_safety_categories", "languages", "list_type", "minimum_option", "score_threshold", "title", "youtube_categories"]
    ALLOWED_SORTS = {
        "items",
        "created_at",
        "updated_at"
    }
    serializer_class = CustomSegmentSerializer
    pagination_class = SegmentPaginator
    queryset = CustomSegment.objects.all().order_by("created_at")

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
        # list type
        list_type = self.request.query_params.get("list_type")
        if list_type:
            value = CustomSegmentSerializer.map_to_id(list_type, item_type="list")
            filters["list_type"] = value
        if filters:
            queryset = queryset.filter(**filters)
        return queryset

    def _do_sorts(self, queryset):
        try:
            sort_by = self.request.query_params["sort_by"]
            if sort_by not in self.ALLOWED_SORTS:
                raise ValidationError("Allowed sorts: {}".format(", ".join(self.ALLOWED_SORTS)))
            if sort_by == "items":
                queryset = queryset.annotate(items=Count("related"))
            queryset = queryset.order_by(sort_by)
        except KeyError:
            pass
        return queryset

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        segment_type = CustomSegmentSerializer.map_to_id(self.kwargs["segment_type"], item_type="segment")
        queryset = super().get_queryset().filter(segment_type=segment_type)
        queryset = self._do_filters(queryset)
        queryset = self._do_sorts(queryset)
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
