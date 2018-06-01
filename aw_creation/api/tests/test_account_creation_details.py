from unittest.mock import patch

from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK

from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.models import *
from userprofile.models import UserSettingsKey
from utils.utils_tests import SingleDatabaseApiConnectorPatcher


class AccountCreationDetailsAPITestCase(AwReportingAPITestCase):
    details_keys = {
        "video25rate", "video50rate", "video75rate", "video100rate",
        "average_position", "view_through", "conversions", "all_conversions",
        "age", "gender", "device", "delivery_trend", "creative", "ad_network"
    }

    def setUp(self):
        self.user = self.create_test_user()
        self.user.aw_settings[UserSettingsKey.SHOW_CONVERSIONS] = True
        self.user.save()

    def test_success_get(self):
        account = Account.objects.create(id="123", name="")
        ac_creation = AccountCreation.objects.create(
            name="", owner=self.user, account=account,
        )
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        date = timezone.now().date()
        average_position = 2
        impressions = 13
        AdGroupStatistic.objects.create(
            date=date, ad_group=ad_group,
            average_position=average_position, impressions=impressions,
        )
        # --
        url = reverse("aw_creation_urls:account_creation_details",
                      args=(ac_creation.id,))
        with patch(
                "aw_creation.api.serializers.SingleDatabaseApiConnector",
                new=SingleDatabaseApiConnectorPatcher
        ):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            self.details_keys,
        )
        self.assertEqual(data["average_position"], average_position)
