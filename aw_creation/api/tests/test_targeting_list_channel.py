from urllib.parse import urlencode
from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from aw_creation.models import *
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class TargetingListTestCase(ExtendedAPITestCase):

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
        ids = (
            "UC-lHJZR3Gqxm24_Vd_AJ5Yw", "UCZJ7m7EnCNodqnu5SAtg8eQ",
            "UCHkj014U2CQ2Nv0UZeYpE_A", "UCBR8-60-B28hp2BmDPdntcQ",
            "UC2xskkQVFEpLcGFnNSLQY0A", "UCXazgXDIYyWH-yXLAkcrFxw"
        )
        for i, uid in enumerate(ids):
            TargetingItem.objects.create(
                criteria=uid,
                ad_group_creation=ad_group,
                type=TargetingItem.CHANNEL_TYPE,
                is_negative=i % 2,
            )
        url = reverse("aw_creation_urls:optimization_ad_group_targeting",
                      args=(ad_group.id, TargetingItem.CHANNEL_TYPE))
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), len(ids))
        self.assertEqual(
            set(response.data[0].keys()),
            {
                'criteria',
                'is_negative',
                'id',
                'name',
                'thumbnail',
            }
        )
        self.assertIsNotNone(response.data[0]['name'])
        self.assertIsNotNone(response.data[0]['thumbnail'])
        self.assertIs(
            any(i['is_negative'] for i in response.data[:3]), False)
        self.assertIs(
            all(i['is_negative'] for i in response.data[3:]), True)

    def test_success_post(self):
        ad_group = self.create_ad_group()
        for i in range(10):
            TargetingItem.objects.create(
                criteria="channel_id_{}".format(i),
                ad_group_creation=ad_group,
                type=TargetingItem.CHANNEL_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.CHANNEL_TYPE),
        )

        data = [
            {'criteria': 'another_channel_1'},
            {'criteria': 'another_channel_2'},
            {'criteria': 'another_channel_3', "is_negative": True},
            "another_channel_4",
        ]
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(data), content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 14)
        self.assertEqual(
            set(i['criteria'] for i in response.data[:3]),
            {
                'another_channel_1',
                'another_channel_2',
                'another_channel_4',
            }
        )
        self.assertEqual(response.data[8]['criteria'], data[2]['criteria'])
        ad_group.campaign_creation.account_creation.refresh_from_db()
        self.assertIs(
            ad_group.campaign_creation.account_creation.is_changed,
            True,
        )

    def test_success_delete(self):
        ad_group = self.create_ad_group()
        for i in range(10):
            TargetingItem.objects.create(
                criteria="channel_id_{}".format(i),
                ad_group_creation=ad_group,
                type=TargetingItem.CHANNEL_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting",
            args=(ad_group.id, TargetingItem.CHANNEL_TYPE),
        )

        data = [
            {'criteria': 'channel_id_1'},
            {'criteria': 'channel_id_2'},
            {'criteria': 'channel_id_4', "is_negative": True},
            "channel_id_5",
        ]
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.delete(
                url, json.dumps(data), content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 6)
        self.assertEqual(
            set(i['criteria'] for i in response.data),
            {
                'channel_id_0',
                'channel_id_3',
                'channel_id_6',
                'channel_id_7',
                'channel_id_8',
                'channel_id_9',
            }
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
                criteria="channel_id_{}".format(i),
                ad_group_creation=ad_group,
                type=TargetingItem.CHANNEL_TYPE,
                is_negative=i % 2,
            )
        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_export",
            args=(ad_group.id, TargetingItem.CHANNEL_TYPE),
        )
        url = "{}?{}".format(
            str(url),
            urlencode({'auth_token': self.user.auth_token.key}),
        )
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        lines = list(response)
        self.assertEqual(len(lines), 11)

    def test_import_list(self):
        ad_group = self.create_ad_group()

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_import",
            args=(ad_group.id, TargetingItem.CHANNEL_TYPE),
        )
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            with open('aw_creation/fixtures/import_channels_list.csv',
                      'rb') as fp:
                response = self.client.post(url, {'file': fp},
                                            format='multipart')
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        ad_group.campaign_creation.account_creation.refresh_from_db()
        self.assertIs(
            ad_group.campaign_creation.account_creation.is_changed,
            True,
        )

