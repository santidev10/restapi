from datetime import timedelta
from unittest.mock import patch, Mock

from django.core import mail
from django.core.urlresolvers import reverse
from oauth2client.client import HttpAccessTokenRefreshError
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, \
    HTTP_403_FORBIDDEN, HTTP_204_NO_CONTENT
from suds import WebFault

from aw_creation.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import *
from saas.utils_tests import SingleDatabaseApiConnectorPatcher


class AccountCreationSetupAPITestCase(AwReportingAPITestCase):
    def setUp(self):
        self.user = self.create_test_user()
        self.add_custom_user_permission(self.user, "view_media_buying")

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

    def test_success_fail_has_no_permission(self):
        self.remove_custom_user_permission(self.user, "view_media_buying")

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
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

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
                'id', 'name', 'account', 'updated_at', 'campaign_creations',
                'updated_at',
                'is_ended', 'is_approved', 'is_paused',
            }
        )
        campaign_data = data['campaign_creations'][0]
        self.assertEqual(
            set(campaign_data.keys()),
            {
                'id', 'name', 'updated_at', 'start', 'end',
                'budget', 'delivery_method', 'type', 'video_networks',
                'languages',
                'frequency_capping', 'ad_schedule_rules', 'location_rules',
                'devices', 'content_exclusions', 'ad_group_creations',
            }
        )
        self.assertEqual(len(campaign_data['content_exclusions']), 2)
        self.assertEqual(
            set(campaign_data['content_exclusions'][0].keys()),
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
            campaign_data['type'],
            dict(id=CampaignCreation.CAMPAIGN_TYPES[0][0],
                 name=CampaignCreation.CAMPAIGN_TYPES[0][1]),
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
                'targeting', 'max_rate', 'video_ad_format',
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
                   new=lambda *_: "uid_from_aw"):
            response = self.client.patch(
                url, json.dumps(request_data), content_type='application/json',
            )

        self.assertEqual(response.status_code, HTTP_200_OK)
        ac.refresh_from_db()
        self.assertEqual(ac.account.id, "uid_from_aw")
        self.assertEqual(len(mail.outbox), 0)

    def test_success_approve_and_send_tags(self):
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
        # creating of a MCC account
        account = Account.objects.create(id="1", name="")
        account_creation = AccountCreation.objects.create(id="1", name="Hi",
                                                          account=account,
                                                          owner=self.user)
        campaign_creation = CampaignCreation.objects.create(id="1", name="Dol",
                                                            account_creation=account_creation)
        ad_group_creation = AdGroupCreation.objects.create(id="1", name="Mal",
                                                           campaign_creation=campaign_creation)
        ad_creation = AdCreation.objects.create(
            id="1", name="Fal", ad_group_creation=ad_group_creation,
            beacon_third_quartile_2="http://hadler.ua",
            beacon_third_quartile_2_changed=True,
            beacon_vast_3="http://google.com.ua",
            beacon_vast_3_changed=True,
            beacon_vast_1_changed=True,
        )
        ad_group_creation_2 = AdGroupCreation.objects.create(id="2",
                                                             name="Sol",
                                                             campaign_creation=campaign_creation)
        ad_creation_2 = AdCreation.objects.create(
            id="2", name="Hel", ad_group_creation=ad_group_creation_2,
            beacon_first_quartile_1="https://gates.ua?unseal=dark_power",
            beacon_first_quartile_1_changed=True,
            beacon_dcm_1="http://google.com.ua",
            beacon_dcm_1_changed=True,
            beacon_dcm_2_changed=True,
        )
        ad_creation_3 = AdCreation.objects.create(
            id="3", name="Tal", ad_group_creation=ad_group_creation_2,
        )

        # account creation to approve it
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(account_creation.id,))
        response = self.client.patch(
            url, json.dumps(dict(is_approved=True)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(mail.outbox), 1)
        email = mail.outbox[0]
        email_body = email.body
        self.assertIn(ad_creation.unique_name, email_body)
        self.assertIn(ad_creation_2.unique_name, email_body)
        self.assertNotIn(ad_creation_3.unique_name, email_body)

        ad_creation.refresh_from_db()
        self.assertIs(ad_creation.beacon_third_quartile_2_changed, False)
        self.assertIs(ad_creation.beacon_vast_3_changed, False)
        self.assertIs(ad_creation.beacon_vast_1_changed, False)
        ad_creation_2.refresh_from_db()
        self.assertIs(ad_creation_2.beacon_first_quartile_1_changed, False)
        self.assertIs(ad_creation_2.beacon_dcm_1_changed, False)
        self.assertIs(ad_creation_2.beacon_dcm_2_changed, False)

    def test_success_approve_not_sending_tags(self):
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
        # creating of a MCC account
        account = Account.objects.create(id="1", name="")
        account_creation = AccountCreation.objects.create(id="1", name="Hi",
                                                          account=account,
                                                          owner=self.user)
        campaign_creation = CampaignCreation.objects.create(id="1", name="Dol",
                                                            account_creation=account_creation)
        ad_group_creation = AdGroupCreation.objects.create(id="1", name="Mal",
                                                           campaign_creation=campaign_creation)
        AdCreation.objects.create(id="1", name="Fal",
                                  ad_group_creation=ad_group_creation)

        # account creation to approve it
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(account_creation.id,))
        response = self.client.patch(
            url, json.dumps(dict(is_approved=True)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(mail.outbox), 0)

    def test_fail_approve_out_of_date(self):
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
        today = datetime.now()
        ac = self.create_account_creation(self.user,
                                          start=today - timedelta(days=2),
                                          end=today)
        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(ac.id,))

        request_data = dict(
            is_approved=True,
        )
        with patch("aw_creation.api.views.create_customer_account",
                   new=lambda *_: "uid_from_aw"):
            response = self.client.patch(
                url, json.dumps(request_data), content_type='application/json',
            )
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_fail_approve_read_only_permission(self):
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
        fault = Mock()
        fault.faultstring = "[ManagedCustomerServiceError.NOT_AUTHORIZED @ operations[0]]"
        write_operation = Mock(side_effect=WebFault(fault, None))
        with patch("aw_creation.api.views.create_customer_account",
                   new=write_operation):
            response = self.client.patch(
                url, json.dumps(request_data), content_type='application/json',
            )
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
            self.assertIn("error", response.data)

    def test_fail_approve_token_expired(self):
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
        write_operation = Mock(
            side_effect=HttpAccessTokenRefreshError(
                "invalid_grant: Token has been expired or revoked.")
        )
        with patch("aw_creation.api.views.create_customer_account",
                   new=write_operation):
            response = self.client.patch(
                url, json.dumps(request_data), content_type='application/json',
            )
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
            self.assertIn("error", response.data)

    def test_success_update_name(self):
        # creating of a MCC account
        manager = Account.objects.create(id="7155851537",
                                         name="Management Account")
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
        with patch(
                "aw_creation.api.views.update_customer_account") as update_method:
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

    def test_marked_is_disapproved_account(self):
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="",
                                                          owner=self.user,
                                                          account=account, )

        campaign = Campaign.objects.create(id=2, name="", account=account,
                                           cost=100)
        campaign_creation = CampaignCreation.objects.create(id=2,
                                                            campaign=campaign,
                                                            account_creation=account_creation)

        ad_group = AdGroup.objects.create(id=2, campaign=campaign)
        ad_group_creation = AdGroupCreation.objects.create(ad_group=ad_group,
                                                           campaign_creation=campaign_creation)

        ad_1 = Ad.objects.create(id=2, ad_group=ad_group, is_disapproved=True)
        ad_2 = Ad.objects.create(id=3, ad_group=ad_group, is_disapproved=False)
        ad_creation_1 = AdCreation.objects.create(ad=ad_1,
                                                  ad_group_creation=ad_group_creation)
        ad_creation_2 = AdCreation.objects.create(ad=ad_2,
                                                  ad_group_creation=ad_group_creation)

        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(account_creation.id,))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['campaign_creations']), 1)
        self.assertEqual(
            len(response.data['campaign_creations'][0]['ad_group_creations']),
            1)
        ads = response.data['campaign_creations'][0]['ad_group_creations'][0][
            'ad_creations']
        self.assertEqual(len(ads), 2)

        campaign_map = dict((ad['id'], ad) for ad in ads)

        self.assertEqual(campaign_map.keys(),
                         {ad_creation_1.id, ad_creation_2.id})
        self.assertTrue(campaign_map[ad_creation_1.id].get('is_disapproved'))
        self.assertFalse(campaign_map[ad_creation_2.id].get('is_disapproved'))

    def test_enterprise_user_should_be_able_to_edit_account_creation(self):
        user = self.user
        self.remove_custom_user_permission(user, "view_media_buying")
        user.update_permissions_from_plan('enterprise')
        user.save()
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects \
            .create(name="", owner=user, account=account, )

        url = reverse("aw_creation_urls:account_creation_setup",
                      args=(account_creation.id,))

        response = self.client.put(url, dict(name="test name"))

        self.assertEqual(response.status_code, HTTP_200_OK)
