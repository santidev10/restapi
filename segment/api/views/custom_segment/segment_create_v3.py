import json
from uuid import uuid4

from django.core.files.uploadhandler import TemporaryFileUploadHandler
from django.db import transaction
from rest_framework.generics import CreateAPIView
from rest_framework.parsers import MultiPartParser
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.serializers import ValidationError
from rest_framework.status import HTTP_201_CREATED

from audit_tool.models import get_hash_name
from brand_safety.utils import BrandSafetyQueryBuilder
from es_components.iab_categories import IAB_TIER2_SET
from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.models.constants import SourceListType
from segment.models.custom_segment import CustomSegment
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.models.custom_segment_file_upload import CustomSegmentSourceFileUpload
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.utils.utils import validate_boolean
from segment.utils.utils import validate_date
from segment.utils.utils import validate_numeric
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class SegmentCreateApiViewV3(CreateAPIView):
    response_fields = (
        "id", "title", "minimum_views", "minimum_subscribers", "segment_type", "severity_filters", "last_upload_date",
        "content_categories", "languages", "countries", "score_threshold", "sentiment", "pending", "minimum_videos",
        "age_groups", "gender", "is_vetted", "age_groups_include_na", "minimum_views_include_na",
        "minimum_subscribers_include_na", "minimum_videos_include_na", "mismatched_language", "vetted_after",
        "countries_include_na",
    )
    serializer_class = CustomSegmentSerializer
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.custom_target_list_creation"),
            IsAdminUser
        ),
    )
    parser_classes = [MultiPartParser]

    def post(self, request, *args, **kwargs):
        """
        Create CustomSegment, CustomSegmentFileUpload, and execute generate_custom_segment
        """
        request.upload_handlers = [TemporaryFileUploadHandler(request)]
        data = json.loads(request.data["data"])
        try:
            validated_data = self._validate_data(request.user.id, data)
        except SegmentCreationOptionsError as error:
            raise ValidationError(f"Exception trying to create segments: {error}")
        segment_type = validated_data["segment_type"]
        created = []
        response = []
        err = None
        try:
            if segment_type == 2:
                if request.FILES:
                    raise ValidationError("You may only upload a source for one list.")
                # Creation type will be 0-2, inclusive. Serializer expects segment_type of 0 or 1
                for i in range(segment_type):
                    options = validated_data.copy()
                    options["segment_type"] = i
                    segment = self._create(options)
                    created.append((options, segment))
            else:
                with transaction.atomic():
                    segment = self._create(validated_data)
                    if request.FILES:
                        self._create_source(segment, request)
                    created.append((validated_data, segment))
        # pylint: disable=broad-except
        except Exception as error:
            # pylint: enable=broad-except
            CustomSegment.objects.filter(id__in=[item[1].id for item in created]).delete()
            err = error
        if err:
            raise ValidationError(f"Exception trying to create segments: {err}")
        for options, segment in created:
            query_builder = BrandSafetyQueryBuilder(options)
            # Use query_builder.query_params to get mapped values used in Elasticsearch query
            query = {
                "params": query_builder.query_params,
                "body": query_builder.query_body.to_dict()
            }
            CustomSegmentFileUpload.enqueue(query=query, segment=segment)
            generate_custom_segment.delay(segment.id)
            res = self._get_response(query_builder.query_params, segment)
            response.append(res)
        return Response(status=HTTP_201_CREATED, data=response)

    def _validate_data(self, user_id, data):
        """
        Validate request data
        Raise ValidationError on invalid parameters
        :param user_id: int
        :param data: dict
        :return: dict
        """
        try:
            validated = self.validate_options(data)
            validated["segment_type"] = self.validate_segment_type(int(data["segment_type"]))
            validated["owner_id"] = user_id
            validated["title_hash"] = get_hash_name(data["title"].lower().strip())
        except (ValueError, TypeError, AttributeError, KeyError) as error:
            raise SegmentCreationOptionsError(f"{type(error).__name__}: {error}")
        return validated

    @staticmethod
    def validate_segment_type(segment_type):
        """
        Validate request segment_type for creation
        :param segment_type: int
        :return: int
        """
        if not 0 <= segment_type <= 2:
            raise ValueError(f"Invalid list_type: {segment_type}. Must 0-2, inclusive")
        return segment_type

    @staticmethod
    def validate_options(options: dict):
        """
        Copy and validate request options for creation
        :param options: dict
        :return:
        """
        opts = options.copy()
        opts["score_threshold"] = int(opts.get("score_threshold", 0) or 0)
        opts["severity_filters"] = opts.get("severity_filters", {}) or {}
        # validate content categories
        opts["content_categories"] = opts.get("content_categories", [])
        if opts["content_categories"]:
            unique_content_categories = set(opts.get("content_categories"))
            bad_content_categories = list(unique_content_categories - IAB_TIER2_SET)
            if bad_content_categories:
                comma_separated = ", ".join(str(item) for item in bad_content_categories)
                raise ValidationError(detail=f"The following content_categories are invalid: '{comma_separated}'")
        opts["languages"] = opts.get("languages", []) or []
        opts["countries"] = opts.get("countries", []) or []
        opts["sentiment"] = int(opts.get("sentiment", 0) or 0)
        opts["last_upload_date"] = validate_date(opts.get("last_upload_date") or "")
        opts["age_groups"] = [validate_numeric(value) for value in opts.get("age_groups", [])]
        # validate boolean fields
        for field_name in ["minimum_views_include_na", "minimum_videos_include_na", "minimum_subscribers_include_na",
                           "age_groups_include_na", "is_vetted", "mismatched_language", "countries_include_na",]:
            value = opts.get(field_name, None)
            opts[field_name] = validate_boolean(value) if value is not None else None
        # validate all numeric fields
        for field_name in ["minimum_views", "minimum_subscribers", "minimum_videos", "gender"]:
            value = opts.get(field_name, None)
            opts[field_name] = validate_numeric(value) if value is not None else None
        opts["vetted_after"] = validate_date(opts.get("vetted_after") or "")
        opts["content_type"] = opts.get("content_type", None)
        opts["content_quality"] = opts.get("content_quality", None)
        return opts

    def _create(self, data: dict):
        """
        Create segment with CustomSegmentSerializer
        :param data: dict
        :return: CustomSegment
        """
        data["list_type"] = "whitelist"
        serializer = self.serializer_class(data=data)
        serializer.is_valid()
        segment = serializer.save()
        return segment

    def _get_response(self, params, segment):
        """
        Copy params with additional fields for response
        :param segment: CustomSegment
        :param params: dict
        :return:
        """
        res = params.copy()
        res["id"] = segment.id
        res["title"] = segment.title
        res["segment_type"] = segment.segment_type
        res["pending"] = True
        res["statistics"] = {}
        return res

    def _create_source(self, segment, request):
        try:
            source_type = request.query_params.get("source_type", SourceListType.INCLUSION)
            source_type = SourceListType(source_type).value
        except ValueError:
            raise ValidationError(f"Invalid source_type. "
                                  f"Valid values: {SourceListType.INCLUSION.value}, {SourceListType.EXCLUSION.value}")
        source = request.FILES["file"]
        key = segment.get_source_s3_key()
        segment.s3_exporter.export_object_to_s3(source, key)
        source_upload = CustomSegmentSourceFileUpload.objects.create(
            segment=segment,
            source_type=source_type,
            filename=key
        )
        return source_upload


class SegmentCreationOptionsError(Exception):
    pass
