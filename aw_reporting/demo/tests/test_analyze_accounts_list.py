from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_creation.models import AccountCreation
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class AccountNamesAPITestCase(ExtendedAPITestCase):

    def test_success_get(self):
        user = self.create_test_user()

        AccountCreation.objects.create(name="", owner=user)  # this item must not be included into response

        url = reverse("aw_reporting_urls:analyze_accounts_list")

        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.keys()),
                         {'items', 'items_count', 'max_page', 'current_page'})

        self.assertEqual(len(response.data['items']), 1)
        account = response.data['items'][0]
        self.assertEqual(account['id'], 'demo')
