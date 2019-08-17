from django.db.models import Count
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError
from rest_framework.status import HTTP_201_CREATED
import uuid

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
        "updated_at",
        "title"
    }
    serializer_class = CustomSegmentSerializer
    pagination_class = SegmentPaginator
    queryset = CustomSegment.objects.all().order_by("created_at")

    def _do_filters(self, queryset):
        """
        Filter queryset
        """
        filters = {}
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
            if self.request.query_params.get("ascending"):
                sort_by = "{}".format(sort_by)
            else:
                sort_by = "-{}".format(sort_by)
            queryset = queryset.order_by(sort_by)
        except KeyError:
            pass
        return queryset

    def get_queryset(self):
        """
        Prepare queryset to display
        """
        segment_type = CustomSegmentSerializer.map_to_id(self.kwargs["segment_type"], item_type="segment")
        queryset = super().get_queryset().filter(owner=self.request.user, segment_type=segment_type)
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
        validated_data = self._validate_data(data, request, kwargs)
        data.update(validated_data)

        serializer = self.serializer_class(data=data)
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()

        query_builder = BrandSafetyQueryBuilder(data)
        CustomSegmentFileUpload.enqueue(query=query_builder.query_body, segment=segment)
        return Response(status=HTTP_201_CREATED, data=serializer.data)

    def _validate_data(self, data, request, kwargs):
        validated = {}
        self._validate_fields(data)
        self.validate_threshold(data["score_threshold"])
        validated["minimum_option"] = self.validate_numeric(data["minimum_option"])
        validated["score_threshold"] = self.validate_numeric(data["score_threshold"])
        validated["segment_type"] = kwargs["segment_type"]
        validated["owner"] = request.user.id
        validated["title_hash"] = get_hash_name(data["title"].lower().strip())
        validated["youtube_categories"] = BrandSafetyQueryBuilder.map_youtube_categories(data["youtube_categories"])
        return validated

    def _validate_fields(self, fields):
        if set(self.REQUIRED_FIELDS) != set(fields):
            raise ValidationError("Fields must consist of: {}".format(", ".join(self.REQUIRED_FIELDS)))

    @staticmethod
    def validate_threshold(threshold):
        err = None
        if not 0 <= threshold <= 100:
            err = "Score threshold must be between 0 and 100, inclusive."
        return err

    @staticmethod
    def validate_numeric(value):
        formatted = str(value).replace(",", "")
        try:
            to_num = int(formatted)
        except ValueError:
            raise ValidationError("The number: {} is not valid.".format(value))
        return to_num
