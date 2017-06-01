from datetime import datetime, timedelta

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_201_CREATED, \
    HTTP_403_FORBIDDEN

from aw_creation.models import *
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class CampaignListAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_get(self):
        today = datetime.now().date()
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        CampaignCreation.objects.create(
            name="1", account_creation=account_creation,
            start=today, end=today + timedelta(days=20),
        )
        CampaignCreation.objects.create(
            name="2", account_creation=account_creation,
            start=today, end=today + timedelta(days=20),
        )

        url = reverse("aw_creation_urls:optimization_campaign_list",
                      args=(account_creation.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data)

    def test_success_get_demo(self):
        url = reverse("aw_creation_urls:optimization_campaign_list",
                      args=(DEMO_ACCOUNT_ID,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_get_format_check(response.data)

    def perform_get_format_check(self, data):
        self.assertEqual(len(data), 2)
        self.assertEqual(
            set(data[0].keys()),
            {
                'id', 'name',
                'is_approved', 'is_paused',
                'start', 'end',
                'goal_units', 'budget', 'max_rate', 'languages',
                'devices', 'frequency_capping', 'ad_schedule_rules',
                'location_rules',
                'ad_group_creations',
            }
        )

    def test_success_post(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )

        url = reverse("aw_creation_urls:optimization_campaign_list",
                      args=(account_creation.id,))
        post_data = dict()

        response = self.client.post(
            url, json.dumps(post_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_201_CREATED)
        self.assertEqual(
            set(response.data.keys()),
            {
                'id',
                'ad_group_creations',
                'ad_schedule_rules',
                'budget',
                'location_rules',
                'is_approved',
                'frequency_capping',
                'max_rate',
                'name',
                'devices',
                'is_paused',
                'goal_units',
                'languages',
                'end',
                'start',
            }
        )

    def test_fail_post_demo(self):
        url = reverse("aw_creation_urls:optimization_campaign_list",
                      args=(DEMO_ACCOUNT_ID,))
        response = self.client.post(
            url, json.dumps(dict()), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)




