from datetime import timedelta

from django.utils import timezone
from rest_framework.generics import CreateAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError
from rest_framework.status import HTTP_201_CREATED

from audit_tool.models import get_hash_name
from brand_safety.utils import BrandSafetyQueryBuilder
from saas.configs.celery import Queue
from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.models.custom_segment import CustomSegment
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload
from segment.tasks.generate_custom_segment import generate_custom_segment
from segment.utils.utils import validate_threshold


class SegmentCreateApiViewV3(CreateAPIView):
    REQUIRED_FIELDS = ["brand_safety_categories", "languages", "list_type", "minimum_option", "score_threshold", "title", "youtube_categories"]
    serializer_class = CustomSegmentSerializer

    def post(self, request, *args, **kwargs):
        data = request.data
        validated_data = self._validate_data(data, request, kwargs)
        data.update(validated_data)
        creation_type = data["segment_type"]
        to_create = []
        err = None
        try:
            if creation_type == 2:
                # Creation type will be 0-2, inclusive. Serializer expects segment_type of 0 or 1
                for i in range(creation_type):
                    data["segment_type"] = i
                    segment = self._create(data)
                    to_create.append(segment)
            else:
                segment = self._create(data)
                to_create.append(segment)

        except Exception as error:
            CustomSegment.objects.filter(id__in=[item.id for item in to_create]).delete()
            err = error

        if err:
            raise ValidationError(f"Exception trying to create segments: {err}")

        for segment in to_create:
            data["segment_type"] = segment.segment_type
            query_builder = BrandSafetyQueryBuilder(data)
            query = {
                "params": query_builder.query_params,
                "body": query_builder.query_body
            }
            CustomSegmentFileUpload.enqueue(query=query, segment=segment)
            generate_custom_segment.apply_async(args=[segment.id], queue=Queue.SEGMENTS)
        return Response(status=HTTP_201_CREATED)

    def _validate_data(self, data, request, kwargs):
        validated = {}
        self._validate_fields(data)
        validated["segment_type"] = self.validate_numeric(data["segment_type"])
        if not 0 <= validated["segment_type"] <= 2:
            raise ValidationError(f"Invalid list_type: {data['segment_type']}. Must 0-2, inclusive")
        validate_threshold(data["score_threshold"])
        validated["minimum_option"] = self.validate_numeric(data["minimum_option"])
        validated["minimum_views"] = data.get("minimum_views", 1000)
        validated["minimum_subscribers"] = data.get("minimum_subscribers", 1000)
        validated["last_upload_date"] = data.get("last_upload_date", (timezone.now() - timedelta(days=365)).date())
        validated["score_threshold"] = self.validate_numeric(data["score_threshold"])
        validated["segment_type"] = kwargs["segment_type"]
        validated["owner"] = request.user.id
        validated["title_hash"] = get_hash_name(data["title"].lower().strip())
        validated["youtube_categories"] = BrandSafetyQueryBuilder.map_youtube_categories(data["youtube_categories"]),
        validated["sentiment"] = data.get("sentiment", 50)
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

    def _create(self, data):
        serializer = self.serializer_class(data=data)
        serializer.is_valid()
        segment = serializer.save()
        return segment
