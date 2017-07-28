from datetime import datetime, timedelta
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, \
    HTTP_403_FORBIDDEN, HTTP_204_NO_CONTENT, HTTP_404_NOT_FOUND
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from unittest.mock import patch
from aw_reporting.api.tests.base import AwReportingAPITestCase


class AccountCreationSetupAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    @staticmethod
    def create_account_creation(owner, start=None, end=None, is_managed=True):
        account_creation = AccountCreation.objects.create(
            name="Pep",
            owner=owner,
            is_managed=is_managed,
        )

        campaign_creation = CampaignCreation.objects.create(
            name="",
            account_creation=account_creation,
            start=start,
            end=end,
        )
        english, _ = Language.objects.get_or_create(id=1000,
                                                    name="English")
        campaign_creation.languages.add(english)

        # location rule
        geo_target = GeoTarget.objects.create(
            id=0, name="Hell", canonical_name="Hell", country_code="RU",
            target_type="place", status="hot",
        )
        LocationRule.objects.create(
            campaign_creation=campaign_creation,
            geo_target=geo_target,
        )
        FrequencyCap.objects.create(
            campaign_creation=campaign_creation,
            limit=10,
        )
        AdScheduleRule.objects.create(
            campaign_creation=campaign_creation,
            day=1,
            from_hour=6,
            to_hour=18,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
        )
        TargetingItem.objects.create(
            ad_group_creation=ad_group_creation,
            criteria="js",
            type=TargetingItem.KEYWORD_TYPE,
            is_negative=True,
        )
        return account_creation

    def test_success_get(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_account_creation(**defaults)
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(ac.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.perform_details_check(data)

    def test_success_get_demo(self):
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(DEMO_ACCOUNT_ID,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_details_check(response.data)

    def perform_details_check(self, data):
        self.assertEqual(
            set(data.keys()),
            {
                'id', 'name', 'updated_at', 'campaign_creations', 'updated_at',
                'is_ended', 'is_approved', 'is_paused',
            }
        )
        campaign_data = data['campaign_creations'][0]
        self.assertEqual(
            set(campaign_data.keys()),
            {
                'id', 'name', 'updated_at', 'start', 'end',
                'budget', 'delivery_method',  'video_ad_format', 'video_networks', 'languages',
                'frequency_capping', 'ad_schedule_rules', 'location_rules',
                'devices',
                'genders', 'parents', 'age_ranges',
                'content_exclusions',
                'ad_group_creations',
            }
        )
        self.assertEqual(len(campaign_data['content_exclusions']), 2)
        self.assertEqual(
            set(campaign_data['content_exclusions'][0].keys()),
            {'id', 'name'}
        )
        for f in ('age_ranges', 'genders', 'parents'):
            self.assertGreater(len(campaign_data[f]), 0)
            self.assertEqual(
                set(campaign_data[f][0].keys()),
                {'id', 'name'}
            )

        self.assertEqual(len(campaign_data['languages']), 1)
        self.assertEqual(
            campaign_data['languages'][0],
            dict(id=1000, name="English"),
        )
        self.assertEqual(len(campaign_data['location_rules']), 1)
        self.assertEqual(
            set(campaign_data['location_rules'][0].keys()),
            {
                'longitude',
                'radius',
                'latitude',
                'bid_modifier',
                'radius_units',
                'geo_target',
            }
        )
        self.assertEqual(len(campaign_data['devices']), 3)
        self.assertEqual(
            set(campaign_data['devices'][0].keys()),
            {
                'id',
                'name',
            }
        )
        self.assertEqual(
            set(campaign_data['location_rules'][0]['radius_units']),
            {'id', 'name'}
        )
        self.assertEqual(len(campaign_data['frequency_capping']), 1)
        self.assertEqual(
            set(campaign_data['frequency_capping'][0].keys()),
            {
                'event_type',
                'limit',
                'level',
                'time_unit',
            }
        )
        for f in ('event_type', 'level', 'time_unit'):
            self.assertEqual(
                set(campaign_data['frequency_capping'][0][f].keys()),
                {'id', 'name'}
            )

        self.assertGreaterEqual(len(campaign_data['ad_schedule_rules']), 1)
        self.assertEqual(
            set(campaign_data['ad_schedule_rules'][0].keys()),
            {
                'from_hour',
                'from_minute',
                'campaign_creation',
                'to_minute',
                'to_hour',
                'day',
            }
        )
        self.assertEqual(
            campaign_data['video_ad_format'],
            dict(id=CampaignCreation.IN_STREAM_TYPE,
                 name=CampaignCreation.VIDEO_AD_FORMATS[0][1]),
        )
        self.assertEqual(
            campaign_data['delivery_method'],
            dict(id=CampaignCreation.STANDARD_DELIVERY,
                 name=CampaignCreation.DELIVERY_METHODS[0][1]),
        )
        self.assertEqual(
            campaign_data['video_networks'],
            [dict(id=uid, name=n)
             for uid, n in CampaignCreation.VIDEO_NETWORKS],
        )
        ad_group_data = campaign_data['ad_group_creations'][0]
        self.assertEqual(
            set(ad_group_data.keys()),
            {
                'id', 'name', 'updated_at', 'ad_creations',
                'genders', 'parents', 'age_ranges',
                'targeting', 'max_rate',
            }
        )
        for f in ('age_ranges', 'genders', 'parents'):
            if len(ad_group_data[f]) > 0:
                self.assertEqual(
                    set(ad_group_data[f][0].keys()),
                    {'id', 'name'}
                )
        self.assertEqual(
            set(ad_group_data['targeting']),
            {'channel', 'video', 'topic', 'interest', 'keyword'}
        )
        self.assertEqual(
            set(ad_group_data['targeting']['keyword']['negative'][0]),
            {'criteria', 'is_negative', 'type', 'name'}
        )

    def test_success_update(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_account_creation(**defaults)

        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(ac.id,))

        request_data = dict(
            name="New 3344334 name",
            is_paused=True,
            is_ended=True,
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        for k, v in request_data.items():
            self.assertEqual(response.data[k], v)

    def test_fail_approve(self):
        ac = self.create_account_creation(self.user)
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(ac.id,))

        request_data = dict(
            is_approved=True,
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_approve(self):
        # creating of a MCC account
        manager = Account.objects.create(id="11", name="Management Account")
        connection = AWConnection.objects.create(
            email="email@mail.com", refresh_token="****",
        )
        AWConnectionToUserRelation.objects.create(
            connection=connection,
            user=self.user,
        )
        AWAccountPermission.objects.create(
            aw_connection=connection,
            account=manager,
        )

        # account creation to approve it
        ac = self.create_account_creation(self.user)
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(ac.id,))

        request_data = dict(
            is_approved=True,
        )
        with patch("aw_creation.api.views.create_customer_account",
                   new=lambda *_: "uid_from_ad_words"):
            response = self.client.patch(
                url, json.dumps(request_data), content_type='application/json',
            )
            self.assertEqual(response.status_code, HTTP_200_OK)
            ac.refresh_from_db()
            self.assertEqual(ac.account.id, "uid_from_ad_words")

    def test_success_update_name(self):
        # creating of a MCC account
        manager = Account.objects.create(id="7155851537", name="Management Account")
        connection = AWConnection.objects.create(
            email="anna.chumak1409@gmail.com",
            refresh_token="1/MJsHAtsAl1YYus3lMX0Tr_oCFGzHbZn7oupW-2SyAcs",
        )
        AWConnectionToUserRelation.objects.create(
            connection=connection,
            user=self.user,
        )
        AWAccountPermission.objects.create(
            aw_connection=connection,
            account=manager,
        )
        account = Account.objects.create(id="7514485750", name="@")
        account.managers.add(manager)
        account_creation = AccountCreation.objects.create(
            name="Pep",
            owner=self.user,
            account=account,
        )

        # account creation to approve it
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(account_creation.id,))

        request_data = dict(name="Account 15")
        with patch("aw_creation.api.views.update_customer_account") as update_method:
            response = self.client.patch(
                url, json.dumps(request_data), content_type='application/json',
            )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(update_method.call_count, 1)

    def test_fail_disapprove(self):
        account = Account.objects.create(id=1, name="")
        ac = self.create_account_creation(self.user)
        ac.account = account
        ac.is_approved = True
        ac.save()
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(ac.id,))
        request_data = dict(
            is_approved=False,
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_fail_update_demo(self):
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(DEMO_ACCOUNT_ID,))
        response = self.client.patch(
            url, json.dumps(dict(is_paused=True)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_fail_name_validation(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_account_creation(**defaults)
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(ac.id,))
        data = dict(
            name="Campaign '",
        )
        response = self.client.patch(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data['name'][0],
            "# and ' are not allowed for titles",
        )

    def test_success_delete(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_account_creation(**defaults)
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(ac.id,))

        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)

        ac.refresh_from_db()
        self.assertIs(ac.is_deleted, True)

    def test_fail_delete_demo(self):
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(DEMO_ACCOUNT_ID,))
        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)


