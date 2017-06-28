from saas.utils_tests import ExtendedAPITestCase
from aw_reporting.models import *


class AwReportingAPITestCase(ExtendedAPITestCase):

    account_list_header_fields = {
        'id', 'name', 'account_creation', 'end', 'start', 'status', 'weekly_chart',
        'clicks', 'cost', 'impressions', 'video_views', 'video_view_rate', 'ctr_v',
    }

    def create_account(self, user):
        account = Account.objects.create(id="123{}".format(user.id), name="Test account")
        manager = Account.objects.create(id="456{}".format(user.id), name="")
        account.managers.add(manager)

        connection = AWConnection.objects.create(
            email=user.email,
            refresh_token="",
        )
        connection.users.add(user)
        AWAccountPermission.objects.create(
            aw_connection=connection,
            account=manager,
        )
        return account


