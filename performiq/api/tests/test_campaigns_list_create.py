import json
from unittest import mock

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from oauth.models import Account
from oauth.models import DV360Advertiser
from oauth.models import DV360Partner
from oauth.models import Campaign
from oauth.models import OAuthAccount
from performiq.analyzers.constants import DataSourceType
from performiq.api.urls.names import PerformIQPathName
from performiq.models import IQCampaign
from performiq.models.constants import OAuthType
from performiq.utils.constants import CSVFieldTypeEnum
from saas.urls.namespaces import Namespace
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class PerformIQCampaignListCreateTestCase(ExtendedAPITestCase):
    def _get_url(self):
        return reverse(Namespace.PERFORMIQ + ":" + PerformIQPathName.CAMPAIGNS)

    def _create_gads(self, user_id, email):
        oauth_account = OAuthAccount.objects.create(user_id=user_id,
                                                    oauth_type=OAuthType.GOOGLE_ADS.value, email=email)
        account = Account.objects.create()
        account.oauth_accounts.add(oauth_account)
        campaign = Campaign.objects.create(oauth_type=oauth_account.oauth_type, account=account)
        return oauth_account, account, campaign

    def _get_iqcampaign_params(self, params):
        default = dict(
            average_cpv=0,
            average_cpm=0,
            content_categories=[],
            content_quality=[],
            content_type=[],
            exclude_content_categories=[],
            languages=[],
            name="",
            score_threshold=0,
            video_view_rate=0,
            ctr=0,
            active_view_viewability=0,
            video_quartile_100_rate=0,
        )
        default.update(params)
        return default

    def _create_dv360(self, user_id, email):
        oauth_account = OAuthAccount.objects.create(user_id=user_id,
                                                    oauth_type=OAuthType.DV360.value, email=email)
        partner = DV360Partner.objects.create(id=next(int_iterator))
        partner.oauth_accounts.add(oauth_account)
        advertiser = DV360Advertiser.objects.create(id=next(int_iterator), partner=partner)
        advertiser.oauth_accounts.add(oauth_account)
        campaign = Campaign.objects.create(oauth_type=oauth_account.oauth_type, advertiser=advertiser)
        return oauth_account, advertiser, campaign

    def test_permission(self):
        self.create_test_user()
        response = self.client.get(self._get_url())
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

        response = self.client.get(self._get_url() + "?analyzed=true")
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_analyzed_success(self):
        user = self.create_admin_user()
        gads_oauth, account, gads_campaign = self._create_gads(user.id, user.email)
        dv360_oauth, advertiser, dv360_campaign = self._create_dv360(user.id, user.email)
        iq_google = IQCampaign.objects.create(user=user, campaign=gads_campaign)
        iq_dv360 = IQCampaign.objects.create(user=user, campaign=dv360_campaign)
        iq_csv = IQCampaign.objects.create(user=user, params=dict(csv_s3_key="test.csv"))

        expected_keys = {
            "analysis_type",
            "campaign",
            "completed",
            "created",
            "id",
            "name",
            "params",
            "results",
            "started",
            "user"
        }

        response = self.client.get(self._get_url() + "?analyzed=true")
        items = sorted(response.data["items"], key=lambda x: x["id"])
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(items[0]["id"], iq_google.id)
        self.assertEqual(items[0]["analysis_type"], DataSourceType.GOOGLE_ADS.value)
        self.assertEqual(items[1]["id"], iq_dv360.id)
        self.assertEqual(items[1]["analysis_type"], DataSourceType.DV360.value)
        self.assertEqual(items[2]["id"], iq_csv.id)
        self.assertEqual(items[2]["analysis_type"], DataSourceType.CSV.value)
        for item in items:
            self.assertEqual(set(item.keys()), expected_keys)

    def test_non_analyzed_success(self):
        """ Test retrieving Campaigns created from oauth """
        user = self.create_admin_user(f"test_{next(int_iterator)}.com")
        gads_oauth, account, gads_campaign = self._create_gads(user.id, user.email)
        dv360_oauth, advertiser, dv360_campaign = self._create_dv360(user.id, user.email)
        response = self.client.get(self._get_url())
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(user.email == data["google_ads"]["email"] == data["dv360"]["email"])
        self.assertEqual(data["google_ads"]["oauth_account_id"], gads_oauth.id)
        self.assertEqual(data["dv360"]["oauth_account_id"], dv360_oauth.id)
        self.assertIn("oauth_account_synced", data["google_ads"])
        self.assertIn("oauth_account_synced", data["dv360"])
        self.assertFalse(data["google_ads"]["oauth_account_synced"])
        self.assertFalse(data["dv360"]["oauth_account_synced"])
        self.assertEqual(data["google_ads"]["campaigns"][0]["id"], gads_campaign.id)
        self.assertEqual(data["dv360"]["campaigns"][0]["id"], dv360_campaign.id)

    def test_create_iq_gads_success(self):
        """ Test successfully creating IQCampaign for Google ads campaign """
        user = self.create_admin_user(f"test_{next(int_iterator)}.com")
        gads_oauth, account, gads_campaign = self._create_gads(user.id, user.email)
        _params = dict(campaign_id=gads_campaign.id, name="test_csv")
        params = self._get_iqcampaign_params(_params)
        with mock.patch("performiq.api.views.campaigns_list_create.start_analysis.start_analysis_task") \
                as mock_analysis:
            response = self.client.post(self._get_url(), data=json.dumps(params), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(response.data["id"])
        mock_analysis.delay.assert_called_once()

    def test_create_iq_dv360_success(self):
        """ Test successfully creating IQCampaign for DV360 campaign """
        user = self.create_admin_user(f"test_{next(int_iterator)}.com")
        dv360_oauth, advertiser, dv360_campaign = self._create_dv360(user.id, user.email)
        _params = dict(campaign_id=dv360_campaign.id, name="test_csv")
        params = self._get_iqcampaign_params(_params)
        with mock.patch("performiq.api.views.campaigns_list_create.start_analysis.start_analysis_task")\
                as mock_analysis:
            response = self.client.post(self._get_url(), data=json.dumps(params), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(response.data["id"])
        mock_analysis.delay.assert_called_once()

    def test_create_iq_csv_success(self):
        """ Test successfully creating IQCampaign for csv upload """
        self.create_admin_user(f"test_{next(int_iterator)}.com")
        csv_s3_key = "test_s3_key.csv"
        csv_column_mapping = {
            CSVFieldTypeEnum.CTR.value: "A",
            CSVFieldTypeEnum.URL.value: "B",
            CSVFieldTypeEnum.AVERAGE_CPM.value: "C"
        }
        _params = dict(csv_s3_key=csv_s3_key, csv_column_mapping=csv_column_mapping, name="test_csv")
        params = self._get_iqcampaign_params(_params)
        with mock.patch("performiq.api.views.campaigns_list_create.start_analysis.start_analysis_task") \
                as mock_analysis:
            response = self.client.post(self._get_url(), data=json.dumps(params), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        mock_analysis.delay.assert_called_once()

    def test_disabled_accounts_excluded_from_list(self):
        user = self.create_admin_user(f"test_{next(int_iterator)}.com")
        gads_oauth, account, gads_campaign = self._create_gads(user.id, user.email)
        gads_oauth.is_enabled = False
        gads_oauth.save(update_fields=["is_enabled"])
        dv360_oauth, advertiser, dv360_campaign = self._create_dv360(user.id, user.email)
        response = self.client.get(self._get_url())
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(data["google_ads"], {})
        self.assertEqual(data["dv360"]["email"], user.email)
        self.assertEqual(data["dv360"]["oauth_account_id"], dv360_oauth.id)
        self.assertEqual(data["dv360"]["campaigns"][0]["id"], dv360_campaign.id)

    def test_search_lowercase(self):
        user = self.create_admin_user()
        gads_oauth, account, gads_campaign = self._create_gads(user.id, user.email)
        dv360_oauth, advertiser, dv360_campaign = self._create_dv360(user.id, user.email)
        iq_google = IQCampaign.objects.create(user=user, campaign=gads_campaign, name="gads")
        IQCampaign.objects.create(user=user, campaign=dv360_campaign, name="dv360")
        IQCampaign.objects.create(user=user, name="csv")
        response = self.client.get(self._get_url() + "?analyzed=true&search=gads")
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["items_count"], 1)
        self.assertEqual(data["items"][0]["id"], iq_google.id)
        self.assertEqual(data["items"][0]["name"], iq_google.name)

    def test_search_mixed_case(self):
        user = self.create_admin_user()
        gads_oauth, account, gads_campaign = self._create_gads(user.id, user.email)
        dv360_oauth, advertiser, dv360_campaign = self._create_dv360(user.id, user.email)
        IQCampaign.objects.create(user=user, campaign=gads_campaign, name="gads case")
        iq_dv360 = IQCampaign.objects.create(user=user, campaign=dv360_campaign, name="Dv360 TesTing CAse")
        IQCampaign.objects.create(user=user, name="csv case")
        response = self.client.get(self._get_url() + "?analyzed=true&search=testing")
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["items_count"], 1)
        self.assertEqual(data["items"][0]["id"], iq_dv360.id)
        self.assertEqual(data["items"][0]["name"], iq_dv360.name)

    def test_search_upper_case(self):
        user = self.create_admin_user()
        gads_oauth, account, gads_campaign = self._create_gads(user.id, user.email)
        dv360_oauth, advertiser, dv360_campaign = self._create_dv360(user.id, user.email)
        IQCampaign.objects.create(user=user, campaign=gads_campaign, name="gads case")
        IQCampaign.objects.create(user=user, campaign=dv360_campaign, name="Dv360 TesTing CAse")
        iq_csv = IQCampaign.objects.create(user=user, name="csv case", params=dict(csv_s3_key="test.csv"))
        response = self.client.get(self._get_url() + "?analyzed=true&search=CSV")
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data["items_count"], 1)
        self.assertEqual(data["items"][0]["id"], iq_csv.id)
        self.assertEqual(data["items"][0]["name"], iq_csv.name)

    def test_params_score_threshold(self):
        """ Test that score threshold is serialized into original value as it is saved with a mapped value for analysis """
        user = self.create_admin_user(f"test_{next(int_iterator)}.com")
        gads_oauth, account, gads_campaign = self._create_gads(user.id, user.email)
        _params = dict(campaign_id=gads_campaign.id, name="test_csv", score_threshold=2)
        params = self._get_iqcampaign_params(_params)
        with mock.patch("performiq.api.views.campaigns_list_create.start_analysis.start_analysis_task"):
            response = self.client.post(self._get_url(), data=json.dumps(params), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["params"]["score_threshold"], params["score_threshold"])
