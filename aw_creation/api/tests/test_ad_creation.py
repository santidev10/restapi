from datetime import datetime, timedelta
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST,\
    HTTP_403_FORBIDDEN, HTTP_204_NO_CONTENT
from aw_reporting.demo.models import DemoAccount
from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class AdGroupAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def create_ad(self, owner, start=None, end=None, account=None):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=owner, account=account,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="",
            account_creation=account_creation,
            start=start,
            end=end,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
        )
        ad_creation = AdCreation.objects.create(
            name="Test Ad", ad_group_creation=ad_group_creation,
            custom_params_raw='[{"name": "test", "value": "ad"}]'
        )
        return ad_creation

    def test_success_get(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad = self.create_ad(**defaults)
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_format_check(response.data)

    def perform_format_check(self, data):
        self.assertEqual(
            set(data.keys()),
            {
                'id', 'name',  'updated_at',
                'video_url', 'display_url', 'tracking_template', 'final_url',
                'video_ad_format', 'custom_params',
                'companion_banner', 'video_id', 'video_title', 'video_description',
                'video_thumbnail', 'video_channel_title', 'video_duration',
            }
        )
        if len(data["custom_params"]) > 0:
            self.assertEqual(
                set(data["custom_params"][0].keys()),
                {'value', 'name'}
            )

    def test_success_get_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]
        ad = ad_group.children[0]

        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_format_check(response.data)

    def test_fail_update_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]
        ad = ad_group.children[0]

        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))

        response = self.client.patch(
            url, json.dumps({}), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_update(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad = self.create_ad(**defaults)
        account_creation = ad.ad_group_creation.campaign_creation.account_creation
        account_creation.is_deleted = True
        account_creation.save()

        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        with open('aw_creation/fixtures/video_thumbnail.png', 'rb') as fp:
            data = dict(
                name="Ad Group  1",
                final_url="https://wtf.com",
                tracking_template="https://track.com?why",
                custom_params=json.dumps([{"name": "name1", "value": "value2"}, {"name": "name2", "value": "value2"}]),
                companion_banner=fp,
                video_ad_format=AdGroupCreation.BUMPER_AD,
            )
            response = self.client.patch(url, data, format='multipart')
        self.assertEqual(response.status_code, HTTP_200_OK)

        account_creation.refresh_from_db()
        self.assertIs(account_creation.is_deleted, False)

        ad.refresh_from_db()
        self.assertEqual(ad.name, data['name'])
        self.assertEqual(ad.final_url, data['final_url'])
        self.assertEqual(ad.tracking_template, data['tracking_template'])
        self.assertEqual(ad.custom_params, [{"name": "name1", "value": "value2"},
                                            {"name": "name2", "value": "value2"}])
        self.assertIsNotNone(ad.companion_banner)

        ad.ad_group_creation.refresh_from_db()
        self.assertEqual(ad.ad_group_creation.video_ad_format, data["video_ad_format"])

        campaign_creation = ad.ad_group_creation.campaign_creation
        campaign_creation.refresh_from_db()
        self.assertEqual(campaign_creation.bid_strategy_type, CampaignCreation.CPM_STRATEGY)

    def test_success_update_json(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad = self.create_ad(**defaults)
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        data = dict(
            name="Ad Group  1",
            final_url="https://wtf.com",
            tracking_template="https://track.com?why",
            custom_params=[{"name": "name1", "value": "value2"}, {"name": "name2", "value": "value2"}],
        )
        response = self.client.patch(url, json.dumps(data), content_type='application/json')

        self.assertEqual(response.status_code, HTTP_200_OK)
        ad.refresh_from_db()

        for f, v in data.items():
            self.assertEqual(getattr(ad, f), v)

    def test_fail_delete_the_only(self):
        ad = self.create_ad(owner=self.user)
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))
        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_delete(self):
        ad = self.create_ad(owner=self.user)
        AdCreation.objects.create(
            name="",
            ad_group_creation=ad.ad_group_creation,
        )
        url = reverse("aw_creation_urls:ad_creation_setup",
                      args=(ad.id,))

        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)
        ad.refresh_from_db()
        self.assertIs(ad.is_deleted, True)


