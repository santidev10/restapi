from collections import namedtuple

from django.core.paginator import EmptyPage
from django.core.paginator import InvalidPage
from django.core.paginator import Paginator
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError

from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from aw_creation.models import AccountCreation
from aw_reporting.models import CriterionType
from utils.views import validate_date


ScalarFilter = namedtuple("ScalarFilter", "name type")
SORT_MAPPING = {
    "campaign": "campaign_name",
    "ad_group": "ad_group_name",
    "target": "target_name",
}

TARGETING_MAPPING = {
    "all": None, # None value implicitly all CriterionType values
    "age": CriterionType.AGE,
    "gender": CriterionType.GENDER,
    "interest": CriterionType.USER_INTEREST_LIST,
    "keyword": CriterionType.KEYWORD,
    "placement": CriterionType.PLACEMENT,
    "topic": CriterionType.VERTICAL,
}


class AccountTargetingAPIView(APIView):
    RANGE_FILTERS = ("average_cpv", "average_cpm", "margin", "impressions_share", "views_share", "video_view_rate")
    SCALAR_FILTERS = (ScalarFilter("impressions", "int"), ScalarFilter("video_views", "int"))
    SORTS = ("campaign_name", "ad_group_name", "target_name")

    def get(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        params = self.request.query_params
        account_creation = self._get_account_creation(request, pk)
        report = AccountTargetingReport(account_creation.account)
        data = self._get_data(report, params)

        page_size = params.get("size", 25)
        paginator = Paginator(data, page_size)
        res = self._get_paginated_response(paginator, 1)
        return Response(data=res)

    def _get_data(self, report, params):
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
        targeting = self._validate_targeting(params.get("targeting"), list(TARGETING_MAPPING.keys()))

        data = report.get_stats(
            criterion_types=targeting,
            sort_key=kpi_sort,
            statistics_filters=statistics_filters,
            kpi_filters=kpi_filters
        )
        return data

    def _get_account_creation(self, request, pk):
        user = request.user
        try:
            return AccountCreation.objects.user_related(user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404

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
        Get all scalar filters
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
        errs = []
        if not isinstance(value, str):
            errs.append(f"Invalid targeting value: {value}. Must be singular string value.")
        if value not in valid_targeting:
            errs.append(f"Invalid targeting value: {value}. Valid targeting: {valid_targeting}")
        if errs:
            raise ValidationError(errs)
        targeting = TARGETING_MAPPING[value]
        return targeting

    def _get_paginated_response(self, paginator, page):
        try:
            page_items = paginator.page(page)
        except EmptyPage:
            page_items = paginator.page(paginator.num_pages)
        except InvalidPage:
            page_items = paginator.page(1)
        data = {
            "current_page": page_items.number,
            "items": page_items.object_list,
            "items_count": paginator.count,
            "max_page": paginator.num_pages,
        }
        return data
