from datetime import timedelta

from django.core.exceptions import PermissionDenied
from django.utils import timezone
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditGender
from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditContentType
from audit_tool.models import IASHistory
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.languages import LANGUAGES
from brand_safety.models import BadWordCategory
from cache.constants import CHANNEL_AGGREGATIONS_KEY
from cache.models import CacheItem
from channel.api.country_view import CountryListApiView
from es_components.countries import COUNTRIES
from segment.api.serializers import CTLParamsSerializer
from segment.api.serializers import ParamsTemplateSerializer
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import SegmentVettingStatusEnum
from segment.models import ParamsTemplate
from segment.utils.query_builder import SegmentQueryBuilder
from segment.utils.utils import set_user_perm_params
from segment.utils.utils import with_unknown
from userprofile.constants import StaticPermissions
from utils.views import get_object


class SegmentCreateOptionsApiView(APIView):

    def get(self, request, *args, **kwargs):
        """
        Generate segment creation options
        If user has params template permission, respond with existing templates owned by user
        If segment_type in request, will respond with items count in request body filters
        """
        res_data = {
            "options": self._get_options()
        }

        if self.request.user.has_permission(StaticPermissions.BUILD__CTL_PARAMS_TEMPLATE):
            res_data["channel_templates"] = self._get_templates(SegmentTypeEnum.CHANNEL.value)
            res_data["video_templates"] = self._get_templates(SegmentTypeEnum.VIDEO.value)

        get_estimate = request.data.get("segment_type") is not None
        if get_estimate:
            data = set_user_perm_params(request, request.data)
            validator = CTLParamsSerializer(data=data)
            validator.is_valid(raise_exception=True)
            params = validator.validated_data
            query_builder = SegmentQueryBuilder(params)
            str_type = SegmentTypeEnum(params["segment_type"]).name.lower()
            res_data[f"{str_type}_items"] = query_builder.count() or 0
        return Response(status=HTTP_200_OK, data=res_data)

    def delete(self, request, *args, **kwargs):
        """
        deletes CTL ParamsTemplate object for a given id
        """
        if request.user.has_permission(StaticPermissions.BUILD__CTL_PARAMS_TEMPLATE):
            template_id = request.data.get("id", None)
            if not isinstance(template_id, int):
                raise TypeError("id value must be an integer")
            params_template = get_object(ParamsTemplate, id=template_id)
            params_template.delete()
            return Response(status=HTTP_200_OK)
        raise PermissionDenied

    def _get_templates(self, segment_type):
        templates = ParamsTemplate.objects.filter(owner=self.request.user, segment_type=segment_type)
        serializer = ParamsTemplateSerializer(templates, many=True)
        return serializer.data

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

        # Try to get cached options that are also used in other parts of viewiq
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
            "age_groups": with_unknown(options=AuditAgeGroup.ID_CHOICES),
            "brand_safety_categories": [
                {"id": _id, "name": category}
                for _id, category
                in BadWordCategory.get_category_mapping(vettable=True).items()
            ],
            "content_categories": AuditUtils.get_iab_categories(),
            "gender": with_unknown(options=AuditGender.ID_CHOICES),
            "countries": countries,
            "languages": languages,
            "is_vetted": [
                {"id": False, "name": "Include Only Non-Vetted"},
                {"id": True, "name": "Include Only Vetted"},
                {"id": None, "name": "Include All"}
            ],
            "content_type_categories": with_unknown(options=AuditContentType.ID_CHOICES),
            "content_quality_categories": with_unknown(options=AuditContentQuality.ID_CHOICES),
            "ads_stats": ads_stats,
            "latest_ias": IASHistory.get_last_ingested_timestamp() or timezone.now() - timedelta(days=7),
            "vetting_status": [
                {"id": SegmentVettingStatusEnum.NOT_VETTED.value, "name": "Non-Vetted"},
                {"id": SegmentVettingStatusEnum.VETTED_SAFE.value, "name": "Vetted Safe"},
                {"id": SegmentVettingStatusEnum.VETTED_RISKY.value, "name": "Vetted Risky"},
            ]
        }
        return options
