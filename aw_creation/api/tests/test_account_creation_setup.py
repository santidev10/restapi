import json
from datetime import datetime
from datetime import timedelta
from unittest.mock import Mock
from unittest.mock import patch

from django.core import mail
from oauth2client.client import HttpAccessTokenRefreshError
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN
from suds import WebFault

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import AdScheduleRule
from aw_creation.models import CampaignCreation
from aw_creation.models import FrequencyCap
from aw_creation.models import Language
from aw_creation.models import LocationRule
from aw_creation.models import TargetingItem
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import Ad
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import GeoTarget
from saas.urls.namespaces import Namespace
from userprofile.permissions import Permissions
from utils.utittests.generic_test import generic_test
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.sdb_connector_patcher import SingleDatabaseApiConnectorPatcher


class AccountCreationSetupAPITestCase(AwReportingAPITestCase):
    @classmethod
    def setUpClass(cls):
        super(AccountCreationSetupAPITestCase, cls).setUpClass()
        Permissions.sync_groups()

    def _get_url(self, account_id):
        return reverse(Name.CreationSetup.ACCOUNT,
                       [Namespace.AW_CREATION],
                       args=(account_id,))

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_media_buying")

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
        self.user.remove_custom_user_permission("view_media_buying")

        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_account_creation(**defaults)
        url = self._get_url(ac.id)

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
        url = self._get_url(ac.id)

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.perform_details_check(data, extra_account_keys=['sync_at'], extra_campaign_keys=['sync_at', 'target_cpa'])

    def test_success_get_demo(self):
        url = self._get_url(DEMO_ACCOUNT_ID)
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_details_check(response.data)

    def perform_details_check(self, data, extra_account_keys=None, extra_campaign_keys=None):
        account_keys = {
            'id', 'name', 'account', 'updated_at', 'campaign_creations',
            'updated_at',
            'is_ended', 'is_approved', 'is_paused',
        }
        campaign_keys = {
            "ad_group_creations",
            "ad_schedule_rules",
            "budget",
            "content_exclusions",
            "delivery_method",
            "devices",
            "end",
            "frequency_capping",
            "id",
            "is_draft",
            "languages",
            "location_rules",
            "name",
            "start",
            "type",
            "updated_at",
            "video_networks",
            "bid_strategy_type",
        }
        if extra_account_keys is not None:
            account_keys.update(extra_account_keys)
        if extra_campaign_keys is not None:
            campaign_keys.update(extra_campaign_keys)

        self.assertEqual(
            set(data.keys()),
            account_keys
        )
        campaign_data = data['campaign_creations'][0]
        self.assertEqual(
            set(campaign_data.keys()),
            campaign_keys
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
                'id',
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

        url = self._get_url(ac.id)

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
        url = self._get_url(ac.id)

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
        connection = AWConnection.objects.create(email="email@mail.com", refresh_token="****")
        AWConnectionToUserRelation.objects.create(connection=connection, user=self.user)
        AWAccountPermission.objects.create(aw_connection=connection, account=manager)
        # account creation to approve it
        ac = self.create_account_creation(self.user)
        url = self._get_url(ac.id)
        request_data = dict(is_approved=True, mcc_account_id=manager.id)
        with patch("aw_creation.api.views.account_creation_setup.create_customer_account",
                   new=lambda *_: "uid_from_aw"):
            response = self.client.patch(url, json.dumps(request_data), content_type='application/json')
        self.assertEqual(response.status_code, HTTP_200_OK)
        ac.refresh_from_db()
        self.assertEqual(ac.account.id, "uid_from_aw")
        self.assertEqual(len(mail.outbox), 0)

    def test_wrong_mcc_account_id(self):
        manager_one = Account.objects.create(id="11", name="Management Account")
        manager_two = Account.objects.create(id="12", name="Management Account")
        wrong_id = "wron_id"
        connection_one = AWConnection.objects.create(email="email@mail.com", refresh_token="****")
        AWConnectionToUserRelation.objects.create(connection=connection_one, user=self.user)
        AWAccountPermission.objects.create(aw_connection=connection_one, account=manager_one)
        connection_two = AWConnection.objects.create(email="email2@mail.com", refresh_token="****")
        AWConnectionToUserRelation.objects.create(connection=connection_two, user=self.user)
        AWAccountPermission.objects.create(aw_connection=connection_two, account=manager_two)
        ac = self.create_account_creation(self.user)
        url = self._get_url(ac.id)
        request_data = dict(is_approved=True, mcc_account_id=wrong_id)
        with patch("aw_creation.api.views.account_creation_setup.create_customer_account",
                   new=lambda *_: "uid_from_aw"):
            response = self.client.patch(url, json.dumps(request_data), content_type='application/json')
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

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
        account = Account.objects.create(id="1", name="",
                                         skip_creating_account_creation=True)
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
        url = self._get_url(account_creation.id)
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
        account = Account.objects.create(id="1", name="",
                                         skip_creating_account_creation=True)
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
        url = self._get_url(account_creation.id)
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
        url = self._get_url(ac.id)

        request_data = dict(
            is_approved=True,
        )
        with patch("aw_creation.api.views.account_creation_setup.create_customer_account",
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
        url = self._get_url(ac.id)

        request_data = dict(
            is_approved=True,
        )
        fault = Mock()
        fault.faultstring = "[ManagedCustomerServiceError.NOT_AUTHORIZED @ operations[0]]"
        write_operation = Mock(side_effect=WebFault(fault, None))
        with patch("aw_creation.api.views.account_creation_setup.create_customer_account",
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
        url = self._get_url(ac.id)

        request_data = dict(
            is_approved=True,
        )
        write_operation = Mock(
            side_effect=HttpAccessTokenRefreshError(
                "invalid_grant: Token has been expired or revoked.")
        )
        with patch("aw_creation.api.views.account_creation_setup.create_customer_account",
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
        account = Account.objects.create(id="7514485750", name="@",
                                         skip_creating_account_creation=True)
        account.managers.add(manager)
        account_creation = AccountCreation.objects.create(
            name="Pep",
            owner=self.user,
            account=account,
        )

        # account creation to approve it
        url = self._get_url(account_creation.id)

        request_data = dict(name="Account 15")
        with patch(
                "aw_creation.api.views.account_creation_setup.update_customer_account") as update_method:
            response = self.client.patch(
                url, json.dumps(request_data), content_type='application/json',
            )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(update_method.call_count, 1)

    def test_fail_disapprove(self):
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        ac = self.create_account_creation(self.user)
        ac.account = account
        ac.is_approved = True
        ac.save()
        url = self._get_url(ac.id)
        request_data = dict(
            is_approved=False,
        )
        response = self.client.patch(
            url, json.dumps(request_data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_fail_update_demo(self):
        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.patch(
            url, json.dumps(dict(is_paused=True)),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_name_validation(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_account_creation(**defaults)
        url = self._get_url(ac.id)
        data = dict(
            name="#Campaign '",
        )
        response = self.client.patch(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(data['name'], response.data.get('name'))

    def test_success_delete(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ac = self.create_account_creation(**defaults)
        url = self._get_url(ac.id)

        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)

        ac.refresh_from_db()
        self.assertIs(ac.is_deleted, True)

    def test_fail_delete_demo(self):
        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_marked_is_disapproved_account(self):
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
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

        url = self._get_url(account_creation.id)
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
        user.remove_custom_user_permission("view_media_buying")

        self.fill_all_groups(user)

        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects \
            .create(name="", owner=user, account=account, )

        url = self._get_url(account_creation.id)

        response = self.client.put(url, dict(name="test name"))

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_creates_customer_account(self):
        user = self.user
        test_aw_id = "test_aw_id"
        manager = Account.objects.create(id=next(int_iterator))
        connection = AWConnection.objects.create(email="email@mail.com", refresh_token="****", )
        AWConnectionToUserRelation.objects.create(connection=connection, user=user, )
        AWAccountPermission.objects.create(aw_connection=connection, account=manager, )
        self.assertEqual(Account.objects.all().count(), 1)
        self.assertEqual(AccountCreation.objects.all().count(), 1)
        account_creation = AccountCreation.objects.create(account=None, owner=user, name="any")
        url = self._get_url(account_creation.id)
        with patch("aw_creation.api.views.account_creation_setup.create_customer_account", return_value=test_aw_id):
            response = self.client.put(
                url, dict(name=account_creation.name, is_approved=True, mcc_account_id=manager.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        account_creation.refresh_from_db()
        self.assertIsNotNone(account_creation.account)
        self.assertEqual(Account.objects.all().count(), 2)
        self.assertEqual(AccountCreation.objects.all().count(), 2)

    @generic_test([
        (None, (field,), dict())
        for field in ("headline", "description_1", "description_2")
    ])
    def test_error_on_empty_discovery_fields(self, property_field):
        user = self.user
        manager = Account.objects.create(id="11", name="Management Account")
        connection = AWConnection.objects.create(email="email@mail.com", refresh_token="****")
        AWConnectionToUserRelation.objects.create(connection=connection, user=user)
        AWAccountPermission.objects.create(aw_connection=connection, account=manager)
        account_creation = AccountCreation.objects \
            .create(name="Name", owner=user,)
        campaign_creation = CampaignCreation.objects.create(
            id=next(int_iterator),
            account_creation=account_creation,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            id=next(int_iterator),
            campaign_creation=campaign_creation,
            video_ad_format=AdGroupCreation.DISCOVERY_TYPE,
        )
        data = dict(
            name=account_creation.name,
            is_approved=True,
            mcc_account_id=manager.id,
            headline="headline",
            description_1="description_1",
            description_2="description_2",
        )
        data.update({property_field: None})
        AdCreation.objects.create(
            id=next(int_iterator),
            ad_group_creation=ad_group_creation,
        )
        url = self._get_url(account_creation.id)

        with patch("aw_creation.api.views.account_creation_setup.create_customer_account",
                   new=lambda *_: "uid_from_aw"):
            response = self.client.put(url, data)

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
