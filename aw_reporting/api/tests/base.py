from saas.utils_tests import ExtendedAPITestCase
from aw_reporting.models import *


class AwReportingAPITestCase(ExtendedAPITestCase):

    def create_account(self, user):
        account = Account.objects.create(id="123", name="")
        manager = Account.objects.create(id="456", name="")
        account.managers.add(manager)

        connection = AWConnection.objects.create(
            email="test@test.test",
            refresh_token="",
        )
        connection.users.add(user)
        AWAccountPermission.objects.create(
            aw_connection=connection,
            account=manager,
        )
        return account


