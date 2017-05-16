from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_202_ACCEPTED, HTTP_200_OK, \
    HTTP_404_NOT_FOUND
from saas.utils_tests import ExtendedAPITestCase
from django.contrib.auth import get_user_model
from urllib.parse import urlencode
from aw_creation.models import *


class OptimizationTargetingAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_get(self):
        account_creation = AccountCreation.objects.create(
            name="Pep",
            owner=self.user,
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
        ad_group_creation1 = AdGroupCreation.objects.create(
            name="AdGroup without tuning",
            campaign_creation=campaign_creation,
        )
        TargetingItem.objects.create(
            criteria="KW #{}".format(ad_group_creation1),
            ad_group_creation=ad_group_creation1,
            type=TargetingItem.KEYWORD_TYPE,
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
        TargetingItem.objects.create(
            criteria="KW #{}".format(ad_group_creation),
            ad_group_creation=ad_group_creation,
            type=TargetingItem.KEYWORD_TYPE,
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
        TargetingItem.objects.create(
            criteria="KW #{}".format(ad_group_creation),
            ad_group_creation=ad_group_creation,
            type=TargetingItem.KEYWORD_TYPE,
        )
        TargetingItem.objects.create(
            criteria="Duplicate KW",
            ad_group_creation=ad_group_creation,
            type=TargetingItem.KEYWORD_TYPE,
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
        TargetingItem.objects.create(
            criteria="KW #{}".format(ad_group_creation),
            ad_group_creation=ad_group_creation,
            type=TargetingItem.KEYWORD_TYPE,
        )
        TargetingItem.objects.create(
            criteria="Duplicate KW",
            ad_group_creation=ad_group_creation,
            type=TargetingItem.KEYWORD_TYPE,
        )
        AdGroupCreation.objects.create(
            name="AdGroup without tuning",
            campaign_creation=campaign_creation,
        )
        # -------
        base_url = reverse(
            "aw_creation_urls:optimization_targeting",
            args=(account_creation.id,
                  OptimizationTuning.IMPRESSIONS_KPI,
                  TargetingItem.KEYWORD_TYPE))
        response = self.client.get(base_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {'items', 'value'},
        )
        items = response.data['items']
        self.assertEqual(len(items), 5)
        self.assertEqual(
            set(items[0].keys()),
            {
                'name',
                'bigger_than_value',
                'conversions',
                'average_cpv',
                'ctr',
                'impressions',
                'all_conversions',
                'clicks',
                'cost',
                'video_views',
                'criteria',
                'video_view_rate',
                'average_cpm',
                'view_through',
                'ctr_v',
            }
        )
        # filters
        filters = dict(
            campaign_creations=campaign_creation.id,
        )
        url = "{}?{}".format(base_url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 2)

        filters = dict(
            ad_group_creations=ad_group_creation1.id,
        )
        url = "{}?{}".format(base_url, urlencode(filters))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)


