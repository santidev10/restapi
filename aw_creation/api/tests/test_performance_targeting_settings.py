from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_403_FORBIDDEN
from aw_creation.models import AccountCreation, CampaignCreation, \
    AccountOptimizationSetting, CampaignOptimizationSetting
from aw_reporting.models import AWConnectionToUserRelation, AWConnection
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from saas.utils_tests import ExtendedAPITestCase
from decimal import Decimal
import json


class AccountNamesAPITestCase(ExtendedAPITestCase):

    data_keys = {
        "id", "name", "campaign_creations",
        'average_cpv', 'ctr_v',  'ctr',  'video_view_rate', 'average_cpm',
    }

    def test_success_get(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(  # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz", refresh_token=""),
            user=user,
        )
        account_creation = AccountCreation.objects.create(id=1, name="AC", owner=user, is_managed=False)
        CampaignCreation.objects.create(id=1, name="CC", account_creation=account_creation)

        url = reverse("aw_creation_urls:performance_targeting_settings",
                      args=(account_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            self.data_keys,
        )
        self.assertEqual(len(data["campaign_creations"]), 1)
        self.assertEqual(set(data["campaign_creations"][0].keys()), self.data_keys - {"campaign_creations"})

    def test_success_put(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(  # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz", refresh_token=""),
            user=user,
        )
        account_creation = AccountCreation.objects.create(id="1", name="AC", owner=user, is_managed=False)
        CampaignCreation.objects.create(id=1, name="CC", account_creation=account_creation)

        url = reverse("aw_creation_urls:performance_targeting_settings",
                      args=(account_creation.id,))

        request_data = {
            "id": "1",
            "average_cpv": "324.345",
            "ctr_v": "0.34",
            "campaign_creations": [
                {
                    "id": 1,
                    "average_cpm": "20.5",
                    "ctr": "1.008",
                    "video_view_rate": "35.5",
                }
            ]
        }
        response = self.client.put(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            self.data_keys,
        )
        self.assertEqual(data["average_cpv"], request_data["average_cpv"])
        self.assertEqual(data["ctr_v"], request_data["ctr_v"])
        self.assertEqual(len(data["campaign_creations"]), 1)
        campaign_creation = data["campaign_creations"][0]
        self.assertEqual(set(campaign_creation.keys()), self.data_keys - {"campaign_creations"})
        self.assertEqual(campaign_creation["average_cpm"],
                         Decimal(request_data["campaign_creations"][0]["average_cpm"]))
        self.assertEqual(campaign_creation["ctr"], Decimal(request_data["campaign_creations"][0]["ctr"]))
        self.assertEqual(campaign_creation["video_view_rate"],
                         Decimal(request_data["campaign_creations"][0]["video_view_rate"]))

    def test_success_get_pre_saved(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(  # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz", refresh_token=""),
            user=user,
        )
        account_creation = AccountCreation.objects.create(id=1, name="AC", owner=user, is_managed=False)
        campaign_creation = CampaignCreation.objects.create(id=1, name="CC", account_creation=account_creation)
        account_settings = dict(
            average_cpv="0.7",
            average_cpm="10",
            video_view_rate="34",
            ctr="0.034",
            ctr_v="1.05",
        )
        AccountOptimizationSetting.objects.create(item=account_creation, **account_settings)
        campaign_settings = dict(**account_settings)
        campaign_settings['video_view_rate'] = "50.5"
        CampaignOptimizationSetting.objects.create(item=campaign_creation, **campaign_settings)

        url = reverse("aw_creation_urls:performance_targeting_settings",
                      args=(account_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            self.data_keys,
        )
        for k, v in account_settings.items():
            self.assertEqual(data[k], Decimal(v))
        self.assertEqual(len(data["campaign_creations"]), 1)
        camp_resp = data["campaign_creations"][0]
        self.assertEqual(set(camp_resp.keys()), self.data_keys - {"campaign_creations"})
        for k, v in campaign_settings.items():
            self.assertEqual(camp_resp[k], Decimal(v))

    def test_success_get_demo(self):
        self.create_test_user()

        url = reverse("aw_creation_urls:performance_targeting_settings",
                      args=(DEMO_ACCOUNT_ID,))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            self.data_keys,
        )
        self.assertEqual(len(data["campaign_creations"]), 2)
        self.assertEqual(set(data["campaign_creations"][0].keys()), self.data_keys - {"campaign_creations"})

    def test_success_get_demo_data(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(id=1, name="AC", owner=user)
        url = reverse("aw_creation_urls:performance_targeting_settings",
                      args=(account_creation.id,))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            self.data_keys,
        )
        self.assertEqual(data['id'], DEMO_ACCOUNT_ID)
        self.assertEqual(len(data["campaign_creations"]), 2)

    def test_success_put_demo_data(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(id=1, name="AC", owner=user)
        url = reverse("aw_creation_urls:performance_targeting_settings",
                      args=(account_creation.id,))
        response = self.client.put(
            url, json.dumps(dict()), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
