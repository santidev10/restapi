from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from django.conf import settings

from audit_tool.models import AuditCategory
from brand_safety.models import BadWordCategory
from brand_safety.utils import BrandSafetyQueryBuilder
from channel.api.country_view import CountryListApiView
from segment.utils.utils import validate_threshold
from segment.api.views.custom_segment.segment_create_v3 import SegmentCreateApiViewV3


class SegmentCreationOptionsApiView(APIView):
    OPTIONAL_FIELDS = ["countries", "languages", "list_type", "severity_filters", "last_upload_date",
                       "minimum_views", "minimum_subscribers", "sentiment", "segment_type", "score_threshold", "content_categories"]

    def post(self, request, *args, **kwargs):
        try:
            data = self._validate_data(request.data)
        except Exception as err:
            raise ValidationError(detail=str(err))
        query_builder = BrandSafetyQueryBuilder(data)
        result = query_builder.execute()
        data = {
            "items": result.hits.total or 0,
            "options": self._get_options()
        }
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
        unexpected = any(key not in expected for key in received)
        if unexpected:
            err = "Unexpected fields: {}".format(", ".join(set(received) - set(expected)))
        else:
            err = validate_threshold(data.get("score_threshold", 0))
        if err:
            raise ValueError(err)

        options = data.copy()
        options["last_upload_date"] = SegmentCreateApiViewV3.validate_date(data.get("last_upload_date"))
        options["content_categories"] = BrandSafetyQueryBuilder.map_content_categories(data.get("content_categories", []))
        return options
