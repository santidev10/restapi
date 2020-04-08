from collections import namedtuple

from django.core.paginator import EmptyPage
from django.core.paginator import InvalidPage
from django.core.paginator import Paginator
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError

from ads_analyzer.reports.account_targeting_report.constants import ReportType
from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from aw_creation.api.views.media_buying.constants import TARGETING_MAPPING
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.api.views.media_buying.utils import validate_targeting
from utils.views import validate_date


ScalarFilter = namedtuple("ScalarFilter", "name type")


class AccountTargetingAPIView(APIView):
    RANGE_FILTERS = ("average_cpv", "average_cpm", "margin", "impressions_share", "views_share", "video_view_rate")
    SCALAR_FILTERS = (ScalarFilter("impressions", "int"), ScalarFilter("video_views", "int"))
    SORTS = ("campaign_name", "ad_group_name", "target_name")

    def get(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        params = self.request.query_params
        account_creation = get_account_creation(request.user, pk)
        report = AccountTargetingReport(account_creation.account, reporting_type={ReportType.STATS, ReportType.SUMMARY})
        data, _, summary = self._get_report(report, params)

        page_size = params.get("size", 25)
        paginator = Paginator(data, page_size)
        res = self._get_paginated_response(paginator, 1, summary)
        return Response(data=res)

    def _get_report(self, report, params):
        """
        Validate and extract parameters
        :param report:
        :param params:
        :param targeting:
        :return:
        """
        statistics_filters = self._get_statistics_filters(params)
        kpi_filters = self._get_all_filters(params)
        kpi_sort = self._validate_sort(params)
        targeting = validate_targeting(params.get("targeting"), list(TARGETING_MAPPING.keys()))

        data, kpi_filters, summary = report.get_report(
            criterion_types=targeting,
            sort_key=kpi_sort,
            statistics_filters=statistics_filters,
            aggregation_filters=kpi_filters
        )
        return data, kpi_filters, summary

    def _get_statistics_filters(self, params):
        """
        Get filters for statistics before aggregation
        :param params:
        :return:
        """
        statistics_filters = {}
        try:
            from_date, to_date = [validate_date(value) for value in params["date"].split(",")]
            statistics_filters.update({
                "date__gte": from_date,
                "date__lte": to_date,
            })
        except KeyError:
            pass
        return statistics_filters

    def _get_all_filters(self, params):
        """
        Extract all query param filters
        :return: dict
        """
        filters = {
            **self._get_kpi_range_filters(params),
            **self._get_kpi_scalar_filters(params),
        }
        return filters

    def _get_kpi_range_filters(self, params):
        """
        Get all range filters for aggregated targeting statistics
        Expects values to be comma separated min, max range values
        :param params: request query_params
        :return:
        """
        range_filters = {}
        for filter_type in self.RANGE_FILTERS:
            try:
                _min, _max = params[filter_type].split(",")
                range_filters.update({
                    f"{filter_type}__gte": _min,
                    f"{filter_type}__lte": _max,
                })
            except KeyError:
                pass
        return range_filters

    def _get_kpi_scalar_filters(self, params):
        """
        Get all scalar filters for aggregated targeting statistics
        Uses ScalarFilter namedtuple's filter type to determine filter suffix
        :param params: request query_params
        :return:
        """
        scalar_filters = {}
        for _filter in self.SCALAR_FILTERS:
            try:
                value = params[_filter.name]
                if _filter.type == "str":
                    scalar_filters[f"{_filter.name}__icontains"] = value
                elif _filter.type == "int":
                    scalar_filters[f"{_filter.name}__gte"] = value
            except KeyError:
                pass
        return scalar_filters

    def _validate_sort(self, params):
        """
        Extract sort param for aggregated targeting statistics
        Default is to sort by campaign id
        :return: dict
        """
        sort_param = params.get("sort_by", "campaign_name")
        if sort_param.strip("-") not in self.SORTS:
            raise ValidationError(f"Invalid sort_by: {sort_param}. Valid sort_by values: {self.SORTS}")
        return sort_param

    def _validate_targeting(self, value, valid_targeting):
        """
        Validate targeting to retrieve
        :param value: str
        :param valid_targeting: list
        :return:
        """
        errs = []
        if not isinstance(value, str):
            errs.append(f"Invalid targeting value: {value}. Must be singular string value.")
        if value not in valid_targeting:
            errs.append(f"Invalid targeting value: {value}. Valid targeting: {valid_targeting}")
        if errs:
            raise ValidationError(errs)
        targeting = TARGETING_MAPPING[value]
        return targeting

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
