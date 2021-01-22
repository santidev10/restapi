from datetime import datetime
from datetime import timedelta
from urllib.parse import urlencode

from django.urls import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.demo.recreate_test_demo_data import recreate_test_demo_data
from utils.unittests.test_case import ExtendedAPITestCase


class TrackAccountsDataAPITestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.Track.DATA)

    @classmethod
    def setUpTestData(cls):
        recreate_test_demo_data()

    def setUp(self):
        self.create_test_user(perms={
            StaticPermissions.CHF_TRENDS: True,
        })

    def test_success_daily(self):
        today = datetime.now().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account = response.data[0]

        self.assertEqual(
            set(account.keys()),
            {
                "id",
                "label",
                "average_1d",
                "average_5d",
                "trend",
            }
        )
        self.assertEqual(len(account["trend"]), 2)

    def test_success_hourly(self):
        today = now_in_default_tz().date()
        filters = dict(
            start_date=today - timedelta(days=2),
            end_date=today - timedelta(days=1),
            indicator="impressions",
            dimension="age",
            breakdown="hourly",
        )
        url = "{}?{}".format(self.url, urlencode(filters))
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 1, "one account")
        account = response.data[0]

        self.assertEqual(
            set(account.keys()),
            {
                "id",
                "label",
                "average_1d",
                "average_5d",
                "trend",
            }
        )
        self.assertEqual(len(account["trend"]), 2 * 24)
