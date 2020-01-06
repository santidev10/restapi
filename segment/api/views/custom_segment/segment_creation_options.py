from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from audit_tool.models import AuditCategory
from brand_safety.models import BadWordCategory
from brand_safety.utils import BrandSafetyQueryBuilder
from channel.api.country_view import CountryListApiView
from segment.utils.utils import validate_threshold
from segment.api.views.custom_segment.segment_create_v3 import SegmentCreateApiViewV3
from segment.models import CustomSegment


class SegmentCreationOptionsApiView(APIView):
    OPTIONAL_FIELDS = ["countries", "languages", "list_type", "severity_filters", "last_upload_date",
                       "minimum_views", "minimum_subscribers", "sentiment", "segment_type", "score_threshold", "content_categories"]

    def post(self, request, *args, **kwargs):
        options = self._validate_data(request.data)
        data = {
            "options": self._get_options()
        }
        get_counts = options["segment_type"] is not None
        if get_counts:
            if options["segment_type"] == 2:
                for int_type in range(options["segment_type"]):
                    str_type = CustomSegment.segment_id_to_type[int_type]
                    options["segment_type"] = int_type
                    query_builder = BrandSafetyQueryBuilder(options)
                    result = query_builder.execute()
                    data[f"{str_type}_items"] = result.hits.total or 0
            else:
                query_builder = BrandSafetyQueryBuilder(options)
                result = query_builder.execute()
                str_type = CustomSegment.segment_id_to_type[options["segment_type"]]
                data[f"{str_type}_items"] = result.hits.total or 0
        status = HTTP_200_OK
        return Response(status=status, data=data)

    def _get_options(self):
        countries = CountryListApiView().get().data
        options = {
            "brand_safety_categories": [
                {"id": _id, "name": category} for _id, category in BadWordCategory.get_category_mapping().items()
            ],
            "content_categories": [
                {"id": _id, "name": category} for _id, category in AuditCategory.get_all(iab=True).items()
            ],
            "countries": countries
        }
        return options

    def _validate_data(self, data):
        expected = self.OPTIONAL_FIELDS
        received = data.keys()
        try:
            unexpected = any(key not in expected for key in received)
            if unexpected:
                raise ValueError("Unexpected fields: {}".format(", ".join(set(received) - set(expected))))
            err = validate_threshold(data.get("score_threshold", 0))
            if err:
                raise ValueError(err)

            if data.get("segment_type") is not None:
                segment_type = int(data["segment_type"])
                if not 0 <= segment_type <= 2:
                    raise ValueError(f"Invalid list_type: {segment_type}. Must 0-2, inclusive")
            else:
                segment_type = None

            if not data.get("brand_safety_categories"):
                data["brand_safety_categories"] = BadWordCategory.objects.values_list("id", flat=True)
        except KeyError as err:
            raise ValidationError(f"Missing required key: {err}")
        except ValueError as err:
            raise ValidationError(f"Invalid value: {err}")
        options = data.copy()
        options["last_upload_date"] = SegmentCreateApiViewV3.validate_date(data.get("last_upload_date"))
        options["content_categories"] = BrandSafetyQueryBuilder.map_content_categories(data.get("content_categories", []))
        options["segment_type"] = segment_type
        return options

    def _validate_segment_type(self, options):
        try:
            segment_type = int(options["segment_type"])
            if not 0 <= segment_type <= 2:
                raise ValueError(f"Invalid list_type: {segment_type}. Must 0-2, inclusive")
        except (KeyError, ValueError) as error:
            raise ValidationError(error)
        return segment_type
