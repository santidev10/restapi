from rest_framework.generics import CreateAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError
from rest_framework.status import HTTP_201_CREATED

from audit_tool.models import get_hash_name
from brand_safety.utils import BrandSafetyQueryBuilder
from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.models.custom_segment import CustomSegment
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.utils.utils import validate_date
from segment.utils.utils import validate_numeric


class SegmentCreateApiViewV3(CreateAPIView):
    response_fields = ("id", "title", "minimum_views", "minimum_subscribers", "segment_type", "list_type", "severity_filters",
                       "content_categories", "languages", "countries", "score_threshold", "sentiment", "pending")
    serializer_class = CustomSegmentSerializer

    def post(self, request, *args, **kwargs):
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
            query = {
                "params": query_builder.query_params,
                "body": query_builder.query_body.to_dict()
            }
            CustomSegmentFileUpload.enqueue(query=query, segment=segment)
            generate_custom_segment.delay(segment.id)
            self._update_response_fields(segment, options)
            res = {
                key: options[key] for key in self.response_fields
            }
            res["statistics"] = {}
            response.append(res)
        return Response(status=HTTP_201_CREATED, data=response)

    def _validate_data(self, user_id: int, data: dict):
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
        if not 0 <= segment_type <= 2:
            raise ValueError(f"Invalid list_type: {segment_type}. Must 0-2, inclusive")
        return segment_type

    @staticmethod
    def validate_options(options: dict):
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
        data["list_type"] = "whitelist"
        serializer = self.serializer_class(data=data)
        serializer.is_valid()
        segment = serializer.save()
        return segment

    def _update_response_fields(self, segment, res):
        """
        Mutate res with additional fields for response
        :param segment: CustomSegment
        :param res: dict
        :return:
        """
        res["id"] = segment.id
        res["pending"] = True


class SegmentCreationOptionsError(Exception):
    pass
