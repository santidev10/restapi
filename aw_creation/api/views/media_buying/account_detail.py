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


ScalarFilter = namedtuple("ScalarFilter", "name type")


class AccountDetailAPIView(APIView):
    RANGE_FILTERS = ("average_cpv", "average_cpm", "margin", "impressions_share", "views_share", "video_view_rate")
    SCALAR_FILTERS = (ScalarFilter("ad_group__campaign__name", "str"), ScalarFilter("impressions", "int"), ScalarFilter("views", "int"))
    SORTS = ("ad_group__campaign__name", "ad_group__name", "target")
    serializer_class = AnalyticsAccountCreationDetailsSerializer

    def get(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        params = self.request.query_params
        filters = self._get_all_filters(params)
        sorts = self._get_sorts(params)
        account_creation = self._get_account_creation(request, pk)
        serializer_context = {
            "targeting_params": {
                "filters": filters,
                "sorts": sorts,
            },
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

    def _get_all_filters(self, params):
        """
        Extract all query param filters
        :return: dict
        """
        filters = {
            **self._get_range_filters(params),
            **self._get_scalar_filters(params),
        }
        return filters

    def _get_range_filters(self, params):
        """
        Get all range filters
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

    def _get_scalar_filters(self, params):
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

    def _get_sorts(self, params):
        """
        Extract all sort params
        Default is to sort by campaign id
        :return: dict
        """
        sort_by = ["ad_group__campaign__name"]
        try:
            # Strip "-" for reverse ordering to check if valid sort
            sort_by = [sort for sort in params["sort_by"].split(",") if sort.strip("-") in self.SORTS]
        except KeyError:
            pass
        return sort_by

    def get_aggregate_total(self, queryset):
        """
        Add aggregate data to response
        :param request:
        :param response:
        :param args:
        :param kwargs:
        :return:
        """
        aggregate_data = queryset \
            .aggreegate(
                impressions=Sum("impressions"),
                views=Sum("views"),
                view_rate=Avg("view_rate"),
                contracted_rate=Avg("contracted_rate"),
                avg_cpm=Avg("avg_cpm"),
                cost=Avg("cost"),
                revenue=Avg("revenue"),

            )
        return aggregate_data
