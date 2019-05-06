from urllib.parse import urlencode

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.models import *
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.models import AdGroup
from utils.utittests.test_case import ExtendedAPITestCase


class DemoTargetingListTestCase(ExtendedAPITestCase):

    @classmethod
    def setUpTestData(cls):
        recreate_demo_data()

    def setUp(self):
        self.user = self.create_test_user()

    def test_export_list(self):
        ad_group = AdGroupCreation.objects.filter(ad_group__campaign__account_id=DEMO_ACCOUNT_ID).first()

        url = reverse(
            "aw_creation_urls:ad_group_creation_targeting_export",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE, "positive"),
        )
        url = "{}?{}".format(
            str(url),
            urlencode({'auth_token': self.user.auth_token.key}),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        lines = list(response)
        self.assertEqual(len(lines), 5)


