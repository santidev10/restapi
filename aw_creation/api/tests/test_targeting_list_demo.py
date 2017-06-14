from urllib.parse import urlencode
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN
from aw_reporting.demo.models import DemoAccount
from aw_creation.models import *
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class DemoTargetingListTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_get(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 8)
        self.assertEqual(
            set(response.data[0].keys()),
            {
                'criteria',
                'type',
                'is_negative',
                'name',
            }
        )

    def test_fail_post(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]

        data = ["KW#{}".format(i) for i in range(10)]
        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )
        response = self.client.post(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_fail_delete(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )

        data = ["KW#{}".format(i) for i in range(5)]
        response = self.client.delete(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_export_list(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_export",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )
        url = "{}?{}".format(
            str(url),
            urlencode({'auth_token': self.user.auth_token.key}),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        lines = list(response)
        self.assertEqual(len(lines), 9)

    def test_import_list(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_import",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )
        with open('aw_creation/fixtures/keywords.csv', 'rb') as fp:
            response = self.client.post(url, {'file': fp},
                                        format='multipart')

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)


