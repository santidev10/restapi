from django.db.models import Sum

from aw_reporting.models import Account
from aw_reporting.tools.trends_tool.base_filters import BaseTrackFiltersList


class TrackFiltersList(BaseTrackFiltersList):
    """
    Lists of the filter names and values
    """

    def _get_accounts(self, user):
        return Account.user_objects(user) \
            .filter(can_manage_clients=False, ) \
            .annotate(impressions=Sum("campaigns__impressions")) \
            .filter(impressions__gt=0) \
            .distinct()
