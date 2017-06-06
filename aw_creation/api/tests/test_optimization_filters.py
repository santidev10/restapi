from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, \
    HTTP_404_NOT_FOUND

from aw_creation.models import *
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
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
            name="Campaign with tuning",
            account_creation=account_creation,
        )
        CampaignOptimizationTuning.objects.create(
            item=campaign_creation,
            kpi=OptimizationTuning.IMPRESSIONS_KPI,
            value="12.345"
        )
        CampaignOptimizationTuning.objects.create(
            item=campaign_creation,
            kpi=OptimizationTuning.VIEW_RATE_KPI,
            value="2.2"
        )
        AdGroupCreation.objects.create(
            name="AdGroup without tuning",
            campaign_creation=campaign_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="AdGroup with tuning",
            campaign_creation=campaign_creation,
        )
        AdGroupOptimizationTuning.objects.create(
            item=ad_group_creation,
            kpi=OptimizationTuning.IMPRESSIONS_KPI,
            value="12.345",
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="Second AdGroup with tuning",
            campaign_creation=campaign_creation,
        )
        AdGroupOptimizationTuning.objects.create(
            item=ad_group_creation,
            kpi=OptimizationTuning.IMPRESSIONS_KPI,
            value="12.345",
        )

        campaign_creation = CampaignCreation.objects.create(
            name="Campaign without tuning",
            account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="AdGroup with tuning",
            campaign_creation=campaign_creation,
        )
        AdGroupOptimizationTuning.objects.create(
            item=ad_group_creation,
            kpi=OptimizationTuning.IMPRESSIONS_KPI,
            value="12.345",
        )
        AdGroupOptimizationTuning.objects.create(
            item=ad_group_creation,
            kpi=OptimizationTuning.VIEW_RATE_KPI,
            value="2.2",
        )
        AdGroupCreation.objects.create(
            name="AdGroup without tuning",
            campaign_creation=campaign_creation,
        )
        return account_creation

    def test_fail_not_found(self):
        url = reverse("aw_creation_urls:optimization_filters",
                      args=(1, OptimizationTuning.IMPRESSIONS_KPI))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_fail_permission(self):
        user = get_user_model().objects.create()
        ac = self.create_account(owner=user)
        url = reverse("aw_creation_urls:optimization_filters",
                      args=(ac.id, OptimizationTuning.IMPRESSIONS_KPI))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success_get(self):
        ac = self.create_account(owner=self.user)
        url = reverse("aw_creation_urls:optimization_filters",
                      args=(ac.id, OptimizationTuning.IMPRESSIONS_KPI))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        first_campaign = response.data[0]
        self.assertEqual(
            set(first_campaign.keys()),
            {'id', 'name', 'ad_group_creations'}
        )
        self.assertEqual(len(first_campaign['ad_group_creations']), 3)
        ad_group_creation = first_campaign['ad_group_creations'][0]
        self.assertEqual(
            set(ad_group_creation.keys()),
            {'id', 'name'}
        )

        second_campaign = response.data[1]
        self.assertEqual(len(second_campaign['ad_group_creations']), 1)

    def test_success_get_demo(self):
        url = reverse("aw_creation_urls:optimization_filters",
                      args=(DEMO_ACCOUNT_ID,
                            OptimizationTuning.IMPRESSIONS_KPI))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        first_campaign = response.data[0]
        self.assertEqual(
            set(first_campaign.keys()),
            {'id', 'name', 'ad_group_creations'}
        )
        ad_group_creation = first_campaign['ad_group_creations'][0]
        self.assertEqual(
            set(ad_group_creation.keys()),
            {'id', 'name'}
        )

