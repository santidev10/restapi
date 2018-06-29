from django.urls import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from aw_reporting.models import AWConnectionToUserRelation, AWConnection
from saas.urls.namespaces import Namespace
from utils.utils_tests import ExtendedAPITestCase


class ConnectAWAccountListTestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.AW_REPORTING + ":" + Name.AWAccounts.LIST)

    def test_success_properties(self):
        user = self.create_test_user()
        aw_connection = AWConnection.objects.create(email=user.email)
        connection = AWConnectionToUserRelation.objects.create(
            id=123,
            connection=aw_connection,
            user=user
        )

        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(set(response.data[0].keys()),
                         {"id", "email", "created", "mcc_accounts",
                          "update_time"})
        self.assertEqual(response.data[0]["id"], connection.id)
