from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN
from urllib.parse import urlencode
from aw_creation.models import *
from saas.utils_tests import ExtendedAPITestCase
from aw_reporting.models import Audience


class InterestTargetingListTestCase(ExtendedAPITestCase):

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
        return ad_group_creation

    def test_success_get(self):
        ad_group = self.create_ad_group()
        for i in range(10):
            Audience.objects.create(
                id=i, name="Interest#{}".format(i),
                type=Audience.IN_MARKET_TYPE,
            )
            TargetingItem.objects.create(
                criteria=i,
                ad_group_creation=ad_group,
                type=TargetingItem.INTEREST_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.INTEREST_TYPE),
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
        account = ad_group.campaign_creation.account_creation
        AccountCreation.objects.filter(
            id=account.id).update(is_changed=False)

        for i in range(10):
            Audience.objects.create(
                id=i, name="Interest#{}".format(i),
                type=Audience.IN_MARKET_TYPE,
            )

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.INTEREST_TYPE),
        )

        data = [i for i in range(20)]
        response = self.client.post(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 10)
        self.assertEqual(
            set(i['criteria'] for i in response.data),
            set(str(i) for i in range(10)),
        )
        self.assertIs(
            any(i['is_negative'] for i in response.data),
            False,
        )
        account.refresh_from_db()
        self.assertIs(account.is_changed, True)

    def test_success_post_negative(self):
        ad_group = self.create_ad_group()
        account = ad_group.campaign_creation.account_creation
        AccountCreation.objects.filter(
            id=account.id).update(is_changed=False)
        for i in range(10):
            Audience.objects.create(
                id=i, name="Interest#{}".format(i),
                type=Audience.IN_MARKET_TYPE,
            )

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.INTEREST_TYPE),
        )

        data = [i for i in range(20)]
        response = self.client.post(
            "{}?is_negative=1".format(url),
            json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 10)
        self.assertEqual(
            set(i['criteria'] for i in response.data),
            set(str(i) for i in range(10)),
        )
        self.assertIs(
            all(i['is_negative'] for i in response.data),
            True,
        )
        account.refresh_from_db()
        self.assertIs(account.is_changed, True)

    def test_success_delete(self):
        ad_group = self.create_ad_group()
        for i in range(10):
            Audience.objects.create(
                id=i, name="Interest#{}".format(i),
                type=Audience.IN_MARKET_TYPE,
            )
            TargetingItem.objects.create(
                criteria=i,
                ad_group_creation=ad_group,
                type=TargetingItem.INTEREST_TYPE,
                is_negative=i % 2,
            )
        account = ad_group.campaign_creation.account_creation
        AccountCreation.objects.filter(
            id=account.id).update(is_changed=False)

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.INTEREST_TYPE),
        )

        data = [i for i in range(5)]

        response = self.client.delete(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 5)
        self.assertEqual(
            set(i['criteria'] for i in response.data),
            {str(i) for i in range(5, 10)}
        )
        account.refresh_from_db()
        self.assertIs(account.is_changed, True)

    def test_export_list(self):
        ad_group = self.create_ad_group()
        for i in range(10):
            Audience.objects.create(
                id=i, name="Interest#{}".format(i),
                type=Audience.IN_MARKET_TYPE,
            )
            TargetingItem.objects.create(
                criteria=i,
                ad_group_creation=ad_group,
                type=TargetingItem.INTEREST_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_export",
            args=(ad_group.id, TargetingItem.INTEREST_TYPE),
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
        for i in range(3):
            Audience.objects.create(
                id=i, name="Interest#{}".format(i),
                type=Audience.IN_MARKET_TYPE,
            )

        account = ad_group.campaign_creation.account_creation
        AccountCreation.objects.filter(
            id=account.id).update(is_changed=False)

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_import",
            args=(ad_group.id, TargetingItem.INTEREST_TYPE),
        )
        with open('aw_creation/fixtures/'
                  'import_topics_list.csv', 'rb') as fp:
            response = self.client.post(url, {'file': fp},
                                        format='multipart')

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

        account.refresh_from_db()
        self.assertIs(account.is_changed, True)

