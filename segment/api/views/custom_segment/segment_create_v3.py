from rest_framework.generics import CreateAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError
from rest_framework.status import HTTP_201_CREATED

from audit_tool.models import AuditProcessor
from audit_tool.models import get_hash_name
from brand_safety.utils import BrandSafetyQueryBuilder
from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.models.custom_segment import CustomSegment
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.utils.utils import validate_date
from segment.utils.utils import validate_numeric


class SegmentCreateApiViewV3(CreateAPIView):
    response_fields = ("id", "title", "minimum_views", "minimum_subscribers", "segment_type", "severity_filters", "last_upload_date",
                       "content_categories", "languages", "countries", "score_threshold", "sentiment", "pending")
    serializer_class = CustomSegmentSerializer

    def post(self, request, *args, **kwargs):
        """
        Create CustomSegment, CustomSegmentFileUpload, and execute generate_custom_segment
        """
        try:
            validated_data = self._validate_data(request.user.id, request.data)
        except SegmentCreationOptionsError as error:
            raise ValidationError(f"Exception trying to create segments: {error}")
        segment_type = validated_data["segment_type"]
        created = []
        response = []
        err = None
        try:
            if segment_type == 2:
                # Creation type will be 0-2, inclusive. Serializer expects segment_type of 0 or 1
                for i in range(segment_type):
                    options = validated_data.copy()
                    options["segment_type"] = i
                    segment = self._create(options)
                    created.append((options, segment))
            else:
                segment = self._create(validated_data)
                created.append((validated_data, segment))
        except Exception as error:
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
            AuditProcessor.objects.create(segment=segment, audit_type=3)
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
            validated["owner"] = user_id
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
        opts["content_categories"] = BrandSafetyQueryBuilder.map_content_categories(opts.get("content_categories", []) or [])
        opts["languages"] = opts.get("languages", []) or []
        opts["countries"] = opts.get("countries", []) or []
        opts["sentiment"] = int(opts.get("sentiment", 0) or 0)
        opts["minimum_views"] = validate_numeric(opts.get("minimum_views", 0) or 0)
        opts["minimum_subscribers"] = validate_numeric(opts.get("minimum_subscribers", 0) or 0)
        opts["last_upload_date"] = validate_date(opts.get("last_upload_date") or "")
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


class SegmentCreationOptionsError(Exception):
    pass
