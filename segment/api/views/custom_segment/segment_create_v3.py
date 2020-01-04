from datetime import datetime

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
from segment.utils.utils import validate_threshold


class SegmentCreateApiViewV3(CreateAPIView):
    response_fields = ("id", "title", "minimum_views", "minimum_subscribers", "segment_type", "list_type",
                       "content_categories", "languages", "countries", "score_threshold", "sentiment")
    serializer_class = CustomSegmentSerializer

    def post(self, request, *args, **kwargs):
        data = request.data
        try:
            validated_data = self._validate_data(request.user.id, data)
            data.update(validated_data)
        except SegmentCreationError as error:
            raise ValidationError(f"Exception trying to create segments: {error}")
        segment_type = data["segment_type"]
        created = []
        response = []
        err = None
        try:
            if segment_type == 2:
                # Creation type will be 0-2, inclusive. Serializer expects segment_type of 0 or 1
                for i in range(segment_type):
                    options = data.copy()
                    options["segment_type"] = i
                    segment = self._create(options)
                    created.append((options, segment))
            else:
                segment = self._create(data)
                created.append((data, segment))
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
            options["id"] = segment.id
            res = {
                key: options[key] for key in self.response_fields
            }
            res["statistics"] = {}
            response.append(res)
        return Response(status=HTTP_201_CREATED, data=response)

    def _validate_data(self, user_id: int, data: dict):
        validated = {}
        try:
            if not 0 <= data["segment_type"] <= 2:
                raise ValueError(f"Invalid list_type: {data['segment_type']}. Must 0-2, inclusive")
            validate_threshold(data["score_threshold"])
            validated["minimum_views"] = self.validate_numeric(data.get("minimum_views", 0))
            validated["minimum_subscribers"] = self.validate_numeric(data.get("minimum_subscribers", 0))
            validated["last_upload_date"] = self.validate_date(data.get("last_upload_date"))
            validated["score_threshold"] = int(data.get("score_threshold", 0))
            validated["sentiment"] = int(data.get("sentiment", 0))
            validated["owner"] = user_id
            validated["title_hash"] = get_hash_name(data["title"].lower().strip())
            validated["content_categories"] = BrandSafetyQueryBuilder.map_content_categories(data["content_categories"])
            validated["countries"] = data.get("countries", [])
        except (ValueError, TypeError, AttributeError, KeyError) as error:
            raise SegmentCreationError(f"{type(error).__name__}: {error}")
        return validated

    def _create(self, data: dict):
        data["list_type"] = "whitelist"
        serializer = self.serializer_class(data=data)
        serializer.is_valid()
        segment = serializer.save()
        return segment

    @staticmethod
    def validate_date(date_str: str):
        if date_str:
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                raise SegmentCreationError(f"Invalid date: {date_str}. Date format must be YYYY-MM-DD")
        return date_str

    @staticmethod
    def validate_numeric(value):
        formatted = str(value).replace(",", "")
        try:
            to_num = int(formatted)
        except ValueError:
            raise ValidationError("The number: {} is not valid.".format(value))
        return to_num


class SegmentCreationError(Exception):
    pass
