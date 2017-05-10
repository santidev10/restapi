from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from saas.utils_tests import ExtendedAPITestCase


class AccountNamesAPITestCase(ExtendedAPITestCase):

    def test_success_get(self):
        self.create_test_user()
        url = reverse("aw_reporting_urls:analyze_accounts")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        account = response.data[0]
        self.assertEqual(
            set(account.keys()),
            {
                'is_ongoing',
                'name',
                'id',
                'start_date',
                'end_date',
                'channels_count',
                'videos_count',
            }
        )
        self.assertEqual(account['id'], 'demo')
