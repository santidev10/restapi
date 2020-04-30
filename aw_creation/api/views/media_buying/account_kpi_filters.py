from collections import namedtuple

from rest_framework.response import Response
from rest_framework.views import APIView

from ads_analyzer.reports.account_targeting_report.constants import ReportType
from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from aw_creation.api.views.media_buying.constants import TARGETING_MAPPING
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.api.views.media_buying.utils import validate_targeting


ScalarFilter = namedtuple("ScalarFilter", "name type")


class AccountKPIFiltersAPIView(APIView):
    RANGE_FILTERS = ("average_cpv", "average_cpm", "margin", "impressions_share", "views_share", "video_view_rate")
    SCALAR_FILTERS = (ScalarFilter("impressions", "int"), ScalarFilter("video_views", "int"))
    SORTS = ("campaign_name", "ad_group_name", "target_name")

    def get(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        params = self.request.query_params
        targeting = validate_targeting(params.get("targeting"), list(TARGETING_MAPPING.keys()))
        account_creation = get_account_creation(request.user, pk)
        report = AccountTargetingReport(account_creation.account, reporting_type=ReportType.KPI_FILTERS)
        _, kpi_filters, _ = report.get_report(targeting)
        return Response(data=kpi_filters)
