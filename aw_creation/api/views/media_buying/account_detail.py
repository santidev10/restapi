from collections import namedtuple

from django.db.models import Sum
from django.db.models import Avg

from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response

from aw_creation.models import AccountCreation
from aw_creation.api.serializers.analytics.account_creation_details_serializer import \
    AnalyticsAccountCreationDetailsSerializer
from aw_creation.api.serializers.media_buying.account_serializer import AccountMediaBuyingSerializer
from utils.views import validate_date


ScalarFilter = namedtuple("ScalarFilter", "name type")
SORT_MAPPING = {
    "campaign": "campaign_name",
    "ad_group": "ad_group_name",
    "target": "target_name",
}


class AccountDetailAPIView(APIView):
    RANGE_FILTERS = ("average_cpv", "average_cpm", "margin", "impressions_share", "views_share", "video_view_rate")
    SCALAR_FILTERS = (ScalarFilter("impressions", "int"), ScalarFilter("video_views", "int"))
    SORTS = ("campaign_name", "ad_group_name", "target_name")
    serializer_class = AnalyticsAccountCreationDetailsSerializer

    def get(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        params = self.request.query_params
        statistics_filters = self._get_statistics_filters(params)
        kpi_filters = self._get_all_filters(params)
        kpi_sort = self._validate_sort(params)
        account_creation = self._get_account_creation(request, pk)
        serializer_context = {
            "kpi_params": {
                "filters": kpi_filters,
                "sort": kpi_sort,
            },
            "statistics_filters": statistics_filters,
            "request": request
        }
        data = AccountMediaBuyingSerializer(account_creation, context=serializer_context).data
        return Response(data=data)

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
        sort_param = params.get("sort_by", "campaign")
        if sort_param.strip("-") not in self.SORTS:
            raise ValueError(f"Invalid sort_by: {sort_param}. Valid sort_by values: {self.SORTS}")
        return sort_param
