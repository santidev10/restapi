from urllib.parse import urlencode
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_reporting.demo.models import DemoAccount
from aw_creation.models import *
from utils.utils_tests import ExtendedAPITestCase


class DemoTargetingListTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_export_list(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]

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


