from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, \
    HTTP_404_NOT_FOUND

from aw_creation.models import *
from saas.utils_tests import ExtendedAPITestCase


class OptimizationSettingsAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def create_account(self, owner):
        account_creation = AccountCreation.objects.create(
            name="Pep",
            owner=owner,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="1",
            account_creation=account_creation,
        )
        AdGroupCreation.objects.create(
            name="11",
            campaign_creation=campaign_creation,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="2",
            account_creation=account_creation,
        )
        AdGroupCreation.objects.create(
            name="21",
            campaign_creation=campaign_creation,
        )
        return account_creation

    def test_fail_not_found(self):
        url = reverse("aw_creation_urls:optimization_settings",
                      args=(1, OptimizationTuning.IMPRESSIONS_KPI))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_fail_permission(self):
        user = get_user_model().objects.create()
        ac = self.create_account(owner=user)
        url = reverse("aw_creation_urls:optimization_settings",
                      args=(ac.id, OptimizationTuning.IMPRESSIONS_KPI))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success_get(self):
        ac = self.create_account(owner=self.user)
        url = reverse("aw_creation_urls:optimization_settings",
                      args=(ac.id, OptimizationTuning.IMPRESSIONS_KPI))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {'id', 'name', 'campaign_creations'}
        )
        campaign_creation = response.data['campaign_creations'][0]
        self.assertEqual(
            set(campaign_creation.keys()),
            {'id', 'name', 'value', 'ad_group_creations'}
        )
        ad_group_creation = campaign_creation['ad_group_creations'][0]
        self.assertEqual(
            set(ad_group_creation.keys()),
            {'id', 'name', 'value'}
        )

    def test_success_update(self):
        account_creation = AccountCreation.objects.create(
            name="Pep",
            owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="1",
            account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="11",
            campaign_creation=campaign_creation,
        )

        url = reverse("aw_creation_urls:optimization_settings",
                      args=(account_creation.id,
                            OptimizationTuning.IMPRESSIONS_KPI))
        data = dict(
            campaign_creations=[
                {'id': campaign_creation.id, 'value': '12.345'},
            ],
            ad_group_creations=[
                {'id': ad_group_creation.id, 'value': '666.666'},
            ],
        )
        response = self.client.put(
            url, json.dumps(data),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        campaign_creation = response.data['campaign_creations'][0]
        self.assertEqual(
            str(campaign_creation['value']),
            data['campaign_creations'][0]['value'],
        )
        self.assertEqual(
            str(campaign_creation['ad_group_creations'][0]['value']),
            data['ad_group_creations'][0]['value'],
        )
