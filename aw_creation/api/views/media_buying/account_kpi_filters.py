from collections import namedtuple

from rest_framework.response import Response
from rest_framework.views import APIView

from ads_analyzer.reports.account_targeting_report.create_report import AccountTargetingReport
from aw_creation.api.views.media_buying.constants import REPORT_CONFIG
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.api.views.media_buying.utils import validate_targeting
from userprofile.constants import StaticPermissions

ScalarFilter = namedtuple("ScalarFilter", "name type")


class AccountKPIFiltersAPIView(APIView):
    """
    GET: Retrieve kpi_filters for aggregated targeting statistics

    """
    permission_classes = (StaticPermissions()(StaticPermissions.MEDIA_BUYING),)

    def get(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        params = self.request.query_params
        config = validate_targeting(params.get("targeting"), list(REPORT_CONFIG.keys()))
        account_creation = get_account_creation(request.user, pk)
        report = AccountTargetingReport(account_creation.account, config["criteria"])
        report.prepare_report(aggregation_columns=config["aggregations"])
        kpi_filters = report.get_kpi_filters()
        return Response(data=kpi_filters)
