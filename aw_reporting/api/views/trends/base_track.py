from datetime import datetime

from rest_framework.views import APIView

from aw_reporting.api.views.trends.constants import INDICATORS
from aw_reporting.models import DATE_FORMAT
from userprofile.constants import StaticPermissions


class TrackApiBase(APIView):
    permission_classes = (StaticPermissions()(StaticPermissions.CHF_TRENDS),)

    def get_filters(self):
        data = self.request.query_params
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        accounts = data.get("accounts")
        accounts = accounts.split("-") if accounts else None
        apex_deal = data.get("apex_deal")
        account_id_str = data.get("account")
        filters = dict(
            account=int(account_id_str) if isinstance(account_id_str,
                                                      str) and account_id_str.isnumeric() else account_id_str,
            accounts=accounts,
            campaign=data.get("campaign"),
            indicator=data.get("indicator", INDICATORS[0][0]),
            breakdown=data.get("breakdown"),
            dimension=data.get("dimension"),
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
        )
        if apex_deal is not None:
            filters["apex_deal"] = apex_deal == "1"
        return filters
