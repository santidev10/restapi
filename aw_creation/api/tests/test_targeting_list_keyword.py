from urllib.parse import urlencode
from django.utils import timezone
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.models import *
from saas.utils_tests import ExtendedAPITestCase


class KeywordTargetingListTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def create_ad_group(self):
        account = AccountCreation.objects.create(
            id="1", name="", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            account_creation=account, name="",
        )
        ad_group_creation = AdGroupCreation.objects.create(
            id="1", name="",
            campaign_creation=campaign_creation,
        )
        AccountCreation.objects.filter(pk=account.id).update(sync_at=timezone.now())
        account.refresh_from_db()
        self.assertEqual(account.is_changed, False)
        return ad_group_creation

    def test_success_get(self):
        ad_group = self.create_ad_group()
        for i in range(10):
            TargetingItem.objects.create(
                criteria=i,
                ad_group_creation=ad_group,
                type=TargetingItem.KEYWORD_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 10)
        self.assertEqual(
            set(response.data[0].keys()),
            {
                'criteria',
                'is_negative',
                'name',
            }
        )
        self.assertIs(
            any(i['is_negative'] for i in response.data[:5]), False)
        self.assertIs(
            all(i['is_negative'] for i in response.data[5:]), True)

    def test_success_post(self):
        ad_group = self.create_ad_group()
        data = ["KW#{}".format(i) for i in range(10)]
        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )
        response = self.client.post(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 10)
        self.assertEqual(
            set(i['criteria'] for i in response.data),
            set(data),
        )
        self.assertIs(
            any(i['is_negative'] for i in response.data),
            False,
        )
        ad_group.campaign_creation.account_creation.refresh_from_db()
        self.assertIs(
            ad_group.campaign_creation.account_creation.is_changed,
            True,
        )

    def test_success_post_negative(self):
        ad_group = self.create_ad_group()
        data = ["KW#{}".format(i) for i in range(10)]
        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )
        response = self.client.post(
            "{}?is_negative=1".format(url),
            json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 10)
        self.assertEqual(
            set(i['criteria'] for i in response.data),
            set(data),
        )
        self.assertIs(
            all(i['is_negative'] for i in response.data),
            True,
        )
        ad_group.campaign_creation.account_creation.refresh_from_db()
        self.assertIs(
            ad_group.campaign_creation.account_creation.is_changed,
            True,
        )

    def test_success_delete(self):
        ad_group = self.create_ad_group()
        for i in range(10):
            TargetingItem.objects.create(
                criteria="KW#{}".format(i),
                ad_group_creation=ad_group,
                type=TargetingItem.KEYWORD_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )

        data = ["KW#{}".format(i) for i in range(5)]
        response = self.client.delete(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 5)
        self.assertEqual(
            set(i['criteria'] for i in response.data),
            {"KW#{}".format(i) for i in range(5, 10)}
        )
        ad_group.campaign_creation.account_creation.refresh_from_db()
        self.assertIs(
            ad_group.campaign_creation.account_creation.is_changed,
            True,
        )

    def test_export_list(self):
        ad_group = self.create_ad_group()
        for i in range(10):
            TargetingItem.objects.create(
                criteria="KW#{}".format(i),
                ad_group_creation=ad_group,
                type=TargetingItem.KEYWORD_TYPE,
                is_negative=i % 2,
            )

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
        self.assertEqual(len(lines), 11)

    def test_import_list(self):
        ad_group = self.create_ad_group()

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_import",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )
        with open('aw_creation/fixtures/keywords.csv', 'rb') as fp:
            response = self.client.post(url, {'file': fp},
                                        format='multipart')

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        ad_group.campaign_creation.account_creation.refresh_from_db()
        self.assertIs(
            ad_group.campaign_creation.account_creation.is_changed,
            True,
        )

