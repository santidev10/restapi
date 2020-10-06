from django.contrib.postgres.fields.jsonb import KeyTextTransform
from django.db.models import IntegerField
from django.db.models import Q
from django.db.models.functions import Cast
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST

from audit_tool.models import get_hash_name
from saas.configs.celery import Queue
from segment.api.paginator import SegmentPaginator
from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import SourceListType
from segment.models.custom_segment import CustomSegment
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.utils.utils import validate_threshold
from segment.utils.query_builder import SegmentQueryBuilder


class SegmentListCreateApiViewV2(ListCreateAPIView):
    REQUIRED_FIELDS = [
        "brand_safety_categories", "languages", "list_type", "minimum_option", "score_threshold", "title",
        "youtube_categories"
    ]
    ALLOWED_SORTS = ["items", "created_at", "updated_at", "title"]
    serializer_class = CustomSegmentSerializer
    pagination_class = SegmentPaginator
    queryset = CustomSegment.objects.all().select_related("export").order_by("created_at")

    def _do_filters(self, queryset):
        """
        Filter queryset

        :return: Queryset
        """
        filters = {}
        q_filter = Q()

        search = self.request.query_params.get("search")
        if search:
            filters["title__icontains"] = search

        list_type = self.request.query_params.get("list_type")
        if list_type:
            value = SourceListType[list_type.upper()].value
            filters["list_type"] = value

        content_categories = self.request.query_params.get("general_data.iab_categories")
        if content_categories:
            for category in content_categories.split(","):
                q_filter |= Q(export__query__params__content_categories__icontains=category)

        # Channel and video language query param differ. Filter out empty str items
        languages = self.request.query_params.get("general_data.top_lang_code", "").split(",") \
                    + self.request.query_params.get("general_data.lang_code", "").split(",")
        languages = list(filter(bool, languages))
        if languages:
            for lang in languages:
                q_filter |= Q(export__query__params__languages__icontains=lang)
        queryset = queryset.filter(q_filter, **filters)
        return queryset

    def _do_sorts(self, queryset):
        """
        Sort queryset

        :param queryset: Queryset
        :return: Queryset
        """
        try:
            sort_by = self.request.query_params["sort_by"]
            if sort_by not in self.ALLOWED_SORTS:
                raise ValidationError("Allowed sorts: {}".format(", ".join(self.ALLOWED_SORTS)))
            if sort_by == "items":
                queryset = queryset.annotate(items=Cast(KeyTextTransform("items_count", "statistics"), IntegerField()))
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

        :return: Queryset
        """
        segment_type = SegmentTypeEnum[self.kwargs["segment_type"].upper()].value
        # Filter queryset depending on permission level
        user = self.request.user
        if user.has_perm("userprofile.vet_audit_admin"):
            base_filters = {}
        elif user.has_perm("userprofile.vet_audit"):
            base_filters = {"audit_id__isnull": False}
        else:
            base_filters = {"owner": self.request.user}
        queryset = super().get_queryset().filter(**base_filters, segment_type=segment_type)
        queryset = self._do_filters(queryset)
        queryset = self._do_sorts(queryset)
        return queryset

    def paginate_queryset(self, queryset):
        """
        Processing flat query param

        :return: Queryset
        """
        flat = self.request.query_params.get("flat")
        if flat == "1":
            return None
        return super().paginate_queryset(queryset)

    def post(self, request, *args, **kwargs):
        """
        Validate request body, create CustomSegment and CustomSegmentFileUpload, invoke generate_custom_segment
        """
        data = request.data
        validated_data = self._validate_data(data, request, kwargs)
        data.update(validated_data)

        try:
            serializer = self.serializer_class(data=data)
            serializer.is_valid()
            segment = serializer.save()
        except ValueError as e:
            return Response(status=HTTP_400_BAD_REQUEST, data=str(e))

        query_builder = SegmentQueryBuilder(data)
        CustomSegmentFileUpload.enqueue(query=query_builder.query_body, segment=segment)
        generate_custom_segment.apply_async(args=[serializer.data["id"]], queue=Queue.SEGMENTS)
        return Response(status=HTTP_201_CREATED, data=serializer.data)

    def _validate_data(self, data, request, kwargs):
        validated = {}
        self._validate_fields(data)
        validate_threshold(data["score_threshold"])
        validated["minimum_option"] = self.validate_numeric(data["minimum_option"])
        validated["score_threshold"] = self.validate_numeric(data["score_threshold"])
        validated["segment_type"] = kwargs["segment_type"]
        validated["owner"] = request.user.id
        validated["title_hash"] = get_hash_name(data["title"].lower().strip())
        validated["youtube_categories"] = SegmentQueryBuilder.map_content_categories(data["youtube_categories"])
        return validated

    def _validate_fields(self, fields):
        if set(self.REQUIRED_FIELDS) != set(fields):
            raise ValidationError("Fields must consist of: {}".format(", ".join(self.REQUIRED_FIELDS)))

    @staticmethod
    def validate_numeric(value):
        formatted = str(value).replace(",", "")
        try:
            to_num = int(formatted)
        except ValueError:
            raise ValidationError("The number: {} is not valid.".format(value))
        return to_num
