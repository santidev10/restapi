import hashlib
import json
from collections import namedtuple

from django.core.paginator import EmptyPage
from django.core.paginator import InvalidPage
from django.core.paginator import Paginator
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView

from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from ads_analyzer.reports.account_targeting_report.export_report import account_targeting_export
from aw_creation.api.views.media_buying.constants import REPORT_CONFIG
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.api.views.media_buying.utils import validate_targeting
from aw_reporting.google_ads.utils import get_criteria_exists_key
from aw_reporting.models import AdGroupTargeting
from aw_reporting.models import TargetingStatusEnum
from userprofile.constants import StaticPermissions
from utils.views import validate_date

ScalarFilter = namedtuple("ScalarFilter", "name type")


class AccountTargetingAPIView(APIView):
    """
    GET: Retrieve AdGroup targeting statistics for Account

    """
    CACHE_KEY_PREFIX = "restapi.aw_creation.views.media_buying.account_targeting"
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MEDIA_BUYING),)

    def get(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        params = self.request.query_params
        account_creation = get_account_creation(request.user, pk)
        account = account_creation.account

        config = validate_targeting(params.get("targeting"), list(REPORT_CONFIG.keys()))
        statistics_filters = self._get_statistics_filters(params)
        kpi_filters = self._get_all_filters(params, config)
        kpi_sort = self._validate_sort(params, config["sorts"])

        if params.get("export"):
            params = {
                "recipient": request.user.email,
                "account_id": account.id,
                "aggregation_columns": config["aggregations"],
                "aggregation_filters": kpi_filters,
                "statistics_filters": statistics_filters,
                "criteria": config["criteria"],
            }
            account_targeting_export.delay(params)
            res = {"message": f"Processing. You will receive an email when your export for: {account.name} is ready."}
        else:
            data, summary = self._get_report(account, config, statistics_filters, kpi_filters, kpi_sort)
            page = params.get("page", 1)
            page_size = params.get("size", 25)
            paginator = Paginator(data, page_size)
            res = self._get_paginated_response(paginator, page, summary)
        return Response(res)

    def _get_report(self, account, config, statistics_filters, kpi_filters, kpi_sort):
        """
        Validate and extract parameters
        :param account:
        :param config:
        :param statistics_filters:
        :param kpi_filters:
        :param kpi_sort:
        :return:
        """
        report = AccountTargetingReport(account, config["criteria"])
        report.prepare_report(
            statistics_filters=statistics_filters,
            aggregation_filters=kpi_filters,
            aggregation_columns=config["aggregations"]
        )
        targeting_data = report.get_targeting_report(sort_key=kpi_sort)
        # Unable to provide overall summary for all targeting with targeting filters because aggregating multiple
        # attribution reports will be inaccurate:
        # https://developers.google.com/adwords/api/docs/guides/reporting#single_and_multiple_attribution
        if config["type"] == "all" and "targeting_status" in kpi_filters:
            summary = None
        else:
            summary = report.get_overall_summary()
        return targeting_data, summary

    def _get_statistics_filters(self, params, search_key="ad_group__campaign__name"):
        """
        Get filters for statistics before aggregation
        :param params:
        :return:
        """
        statistics_filters = {}
        if "search" in params:
            statistics_filters[f"{search_key}__icontains"] = params["search"]
        try:
            from_date, to_date = [validate_date(value) for value in params["date"].split(",")]
            if from_date > to_date:
                raise ValidationError(f"Invalid date range: from: {from_date}, to: {to_date}")
            statistics_filters.update({
                "date__gte": from_date,
                "date__lte": to_date,
            })
        except KeyError:
            pass
        return statistics_filters

    def _get_all_filters(self, params, config):
        """
        Extract all query param filters
        :return: dict
        """
        filters = {
            **self._get_kpi_range_filters(params, config["range_filters"]),
            **self._get_kpi_scalar_filters(params, config["scalar_filters"]),
        }
        return filters

    def _get_kpi_range_filters(self, params, valid_range_filters):
        """
        Get all range filters for aggregated targeting statistics
        Expects values to be comma separated min, max range values
        :param params: request query_params
        :return:
        """
        range_filters = {}
        for filter_type in valid_range_filters:
            try:
                _min, _max = params[filter_type].strip("/").split(",")
                if float(_min) > float(_max):
                    raise ValidationError(f"Invalid {filter_type} range: min: {_min}, max: {_max}")
                range_filters.update({
                    f"{filter_type}__gte": _min,
                    f"{filter_type}__lte": _max,
                })
            except TypeError:
                raise ValidationError(f"Invalid decimal values: {params[filter_type]}")
            except KeyError:
                pass
        return range_filters

    def _get_kpi_scalar_filters(self, params, valid_filters):
        """
        Get all scalar filters for aggregated targeting statistics
        Uses ScalarFilter namedtuple's filter type to determine filter suffix
        :param params: request query_params
        :return:
        """
        scalar_filters = {}
        for _filter in valid_filters:
            try:
                if _filter.type == "str":
                    value = str(params[_filter.name])
                else:
                    value = int(params[_filter.name])
                scalar_filters[f"{_filter.name}{_filter.operator}"] = value
            except KeyError:
                pass
        return scalar_filters

    def _validate_sort(self, params, sorts):
        """
        Extract sort param for aggregated targeting statistics
        Default is to sort by campaign id
        :return: dict
        """
        sort_param = params.get("sort_by", "campaign_name")
        if sort_param.strip("-") not in sorts:
            raise ValidationError(f"Invalid sort_by: {sort_param}. Valid sort_by values: {sorts}")
        return sort_param

    def _get_paginated_response(self, paginator, page, summary):
        """ Paginate statistics """
        try:
            page_items = paginator.page(page)
        except EmptyPage:
            page_items = paginator.page(paginator.num_pages)
        except InvalidPage:
            page_items = paginator.page(1)
        data = {
            "summary": summary,
            "current_page": page_items.number,
            "items": page_items.object_list,
            "items_count": paginator.count,
            "max_page": paginator.num_pages,
        }
        return data

    def _set_targeting_criteria(self, account, targeting_items):
        """
        Copy AdGroupTargeting values from Criteria Performance report to aggregated statistics
        :param targeting_items:
        :return:
        """
        statistic_criteria = [item["criteria"] for item in targeting_items]
        existing_targeting = AdGroupTargeting.objects \
            .filter(ad_group__campaign__account=account, statistic_criteria__in=statistic_criteria)
        exists_mapping = {
            get_criteria_exists_key(
                targeting_obj.ad_group_id, targeting_obj.type_id, targeting_obj.statistic_criteria
            ): targeting_obj
            for targeting_obj in existing_targeting
        }
        for res_item in targeting_items:
            try:
                criteria_key = get_criteria_exists_key(res_item["ad_group_id"], res_item["type"],
                                                       str(res_item["criteria"]))
                targeting_obj = exists_mapping[criteria_key]
                if targeting_obj.status == TargetingStatusEnum.ENABLED.value and targeting_obj.is_negative is True:
                    status = TargetingStatusEnum.EXCLUDED.name
                else:
                    status = TargetingStatusEnum(targeting_obj.status).name
                data = {
                    "targeting_id": targeting_obj.id,
                    "targeting_status": status,
                    "sync_pending": targeting_obj.sync_pending,
                }
            except KeyError:
                data = {
                    "targeting_id": None,
                    "targeting_status": None,
                    "sync_pending": None,
                }
            res_item.update(data)

    def get_cache_key(self, part, options):
        params = {}
        query_params = options[0][1]
        # Get sorted query params for consistency
        for key in sorted(query_params.keys()):
            params[key] = query_params[key]
        data = dict(
            account_creation_id=options[0][0].id,
            query_params=params,
        )
        key_json = json.dumps(data, sort_keys=True, cls=DjangoJSONEncoder)
        key_hash = hashlib.md5(key_json.encode()).hexdigest()
        key = f"{self.CACHE_KEY_PREFIX}.{part}.{key_hash}"
        return key, key_json
