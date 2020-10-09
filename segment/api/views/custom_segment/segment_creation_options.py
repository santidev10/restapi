from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditGender
from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditContentType
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.languages import LANGUAGES
from brand_safety.models import BadWordCategory
from cache.constants import CHANNEL_AGGREGATIONS_KEY
from cache.models import CacheItem
from channel.api.country_view import CountryListApiView
from es_components.countries import COUNTRIES
from segment.api.views.custom_segment.segment_create_v3 import SegmentCreateApiViewV3
from segment.models import CustomSegment
from segment.utils.utils import with_all
from segment.models.constants import SegmentTypeEnum
from segment.utils.query_builder import SegmentQueryBuilder


class SegmentCreationOptionsApiView(APIView):

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
                    str_type = SegmentTypeEnum(int_type).name.lower()
                    options["segment_type"] = int_type
                    query_builder = SegmentQueryBuilder(options)
                    result = query_builder.execute()
                    res_data[f"{str_type}_items"] = result.hits.total.value or 0
            else:
                query_builder = SegmentQueryBuilder(options)
                result = query_builder.execute()
                str_type = SegmentTypeEnum(options["segment_type"]).name.lower()
                res_data[f"{str_type}_items"] = result.hits.total.value or 0
        return Response(status=HTTP_200_OK, data=res_data)

    @staticmethod
    def _get_options():
        """
        Get segment creation options
        Try to get aggregation filters from cache for filters sorted by count

        :return: dict
        """
        ads_stats_keys = ("ctr", "ctr_v", "average_cpm", "average_cpv", "video_view_rate",
                          "video_quartile_100_rate")
        stats_keys = ("last_30day_views",)
        def get_agg_min_max_filter_values(cache, keys, field):
            values = {}
            for key in keys:
                field_key = f"{field}.{key}"
                min_val = cache.get(field_key + ":min", {}).get("value", 0)
                max_val = cache.get(field_key + ":max", {}).get("value", 0)
                values[key] = {
                    "id": key,
                    "min": min_val,
                    "max": max_val,
                }
            return values

        try:
            agg_cache = CacheItem.objects.get(key=CHANNEL_AGGREGATIONS_KEY).value
            countries = [
                {
                    "id": item["key"],
                    "common": COUNTRIES[item["key"]][0]
                }
                for item in agg_cache["general_data.country_code"]["buckets"]
            ]
            lang_codes = [item["key"] for item in agg_cache["general_data.top_lang_code"]["buckets"]]

            languages = []
            for code in lang_codes:
                try:
                    lang = LANGUAGES[code]
                except KeyError:
                    lang = code
                languages.append({"id": code, "title": lang})
            for code, lang in LANGUAGES.items():
                if code not in lang_codes:
                    languages.append({"id": code, "title": lang})

            ads_stats = {
                **get_agg_min_max_filter_values(agg_cache, ads_stats_keys, "ads_stats"),
                **get_agg_min_max_filter_values(agg_cache, stats_keys, "stats"),
            }

        except (CacheItem.DoesNotExist, KeyError):
            countries = CountryListApiView().get().data
            languages = [
                {"id": code, "title": lang}
                for code, lang in LANGUAGES.items()
            ]
            ads_stats = {
                **get_agg_min_max_filter_values({}, ads_stats_keys, "ads_stats"),
                **get_agg_min_max_filter_values({}, stats_keys, "stats"),
            }
        options = {
            "age_groups": [
                {"id": age_group_id, "name": age_group_name} for age_group_id, age_group_name in
                AuditAgeGroup.ID_CHOICES
            ],
            "brand_safety_categories": [
                {"id": _id, "name": category}
                for _id, category
                in BadWordCategory.get_category_mapping(vettable=True).items()
            ],
            "content_categories": AuditUtils.get_iab_categories(),
            "gender": [
                {"id": gender_id, "name": gender_name} for gender_id, gender_name in AuditGender.ID_CHOICES
            ],
            "countries": countries,
            "languages": languages,
            "is_vetted": [
                {"id": False, "name": "Include Only Non-Vetted"},
                {"id": True, "name": "Include Only Vetted"},
                {"id": None, "name": "Include All"}
            ],
            "content_type_categories": with_all(all_options=AuditContentType.ID_CHOICES),
            "content_quality_categories": with_all(all_options=AuditContentQuality.ID_CHOICES),
            "ads_stats": ads_stats,
        }
        return options

    def _validate_data(self, data):
        """
        Validate request body

        :param data: dict
        :return: dict
        """
        try:
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
