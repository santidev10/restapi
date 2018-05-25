from utils.utils_tests import ExtendedAPITestCase
from aw_reporting.models import *
from datetime import datetime
import pytz


class AwReportingAPITestCase(ExtendedAPITestCase):

    account_list_header_fields = {
        'id', 'name', 'account_creation', 'end', 'start', 'status', 'weekly_chart',
        'clicks', 'cost', 'impressions', 'video_views', 'video_view_rate', 'ctr_v',
    }

    def create_account(self, user, prefix="", manager=None):
        now = datetime.now(tz=pytz.utc)
        account = Account.objects.create(
            id="{}123{}".format(prefix, user.id)[-15:],
            name="Test account", update_time=now)
        if manager is None:
            manager = Account.objects.create(
                id="{}456{}".format(prefix, user.id)[-15:], name="")
        account.managers.add(manager)

        connection, _ = AWConnection.objects.get_or_create(
            email=user.email,
            refresh_token="",
        )
        AWConnectionToUserRelation.objects.get_or_create(
            connection=connection,
            user=user,
        )
        AWAccountPermission.objects.get_or_create(
            aw_connection=connection,
            account=manager,
        )
        return account


