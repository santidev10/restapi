from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from audit_tool.models import AuditCategory
from brand_safety.languages import LANG_CODES
from brand_safety.models import BadWordCategory
from brand_safety.utils import BrandSafetyQueryBuilder
from cache.models import CacheItem
from cache.constants import CHANNEL_AGGREGATIONS_KEY
from channel.api.country_view import CountryListApiView
from segment.api.views.custom_segment.segment_create_v3 import SegmentCreateApiViewV3
from segment.models import CustomSegment


class SegmentCreationOptionsApiView(APIView):
    OPTIONAL_FIELDS = ["countries", "languages", "list_type", "severity_filters", "last_upload_date",
                       "minimum_views", "minimum_subscribers", "sentiment", "segment_type", "score_threshold", "content_categories"]

    def post(self, request, *args, **kwargs):
        """
        Generate segment creation options
        If segment_type in request, will respond with items count in request body filters
        """
        options = self._validate_data(request.data)
        res_data = {
            "options": self._get_options()
        }
        # Only get item estimates if valid segment_type
        get_counts = options["segment_type"] is not None
        if get_counts:
            if options["segment_type"] == 2:
                for int_type in range(options["segment_type"]):
                    str_type = CustomSegment.segment_id_to_type[int_type]
                    options["segment_type"] = int_type
                    query_builder = BrandSafetyQueryBuilder(options)
                    result = query_builder.execute()
                    res_data[f"{str_type}_items"] = result.hits.total.value or 0
            else:
                query_builder = BrandSafetyQueryBuilder(options)
                result = query_builder.execute()
                str_type = CustomSegment.segment_id_to_type[options["segment_type"]]
                res_data[f"{str_type}_items"] = result.hits.total.value or 0
        return Response(status=HTTP_200_OK, data=res_data)

    @staticmethod
    def _get_options():
        """
        Get segment creation options
        Try to get aggregation filters from cache for filters sorted by count

        :return: dict
        """
        try:
            agg_cache = CacheItem.objects.get(key=CHANNEL_AGGREGATIONS_KEY)
            countries = [
                {"common": item["key"]}
                for item in agg_cache.value["general_data.country"]["buckets"]
            ]
            lang_str = [item["key"] for item in agg_cache.value['general_data.top_language']['buckets']]

            languages = []
            for lang in lang_str:
                try:
                    code = LANG_CODES[lang]
                except KeyError:
                    code = lang
                languages.append({"id": code, "title": lang})
        except (CacheItem.DoesNotExist, KeyError):
            countries = CountryListApiView().get().data
            languages = [
                {"id": code, "title": lang}
                for lang, code in LANG_CODES.items()
            ]
        options = {
            "brand_safety_categories": [
                {"id": _id, "name": category} for _id, category in BadWordCategory.get_category_mapping().items()
            ],
            "content_categories": [
                {"id": _id, "name": category} for _id, category in AuditCategory.get_all(iab=True, unique=True).items()
            ],
            "countries": countries,
            "languages": languages,
        }
        return options

    def _validate_data(self, data):
        """
        Validate request body

        :param data: dict
        :return: dict
        """
        expected = self.OPTIONAL_FIELDS
        received = data.keys()
        try:
            unexpected = any(key not in expected for key in received)
            if unexpected:
                raise ValueError("Unexpected fields: {}".format(", ".join(set(received) - set(expected))))

            if data.get("segment_type") is not None:
                segment_type = SegmentCreateApiViewV3.validate_segment_type(int(data["segment_type"]))
            else:
                segment_type = None
            options = SegmentCreateApiViewV3.validate_options(data)
            options["segment_type"] = segment_type
        except KeyError as err:
            raise ValidationError(f"Missing required key: {err}")
        except ValueError as err:
            raise ValidationError(f"Invalid value: {err}")
        return options
