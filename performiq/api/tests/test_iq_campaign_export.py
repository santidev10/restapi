from unittest import mock

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND

from performiq.api.urls.names import PerformIQPathName
from performiq.models import IQCampaign
from performiq.models.constants import EXPORT_RESULTS_KEYS
from saas.urls.namespaces import Namespace
from performiq.utils.s3_exporter import PerformS3Exporter
from utils.unittests.test_case import ExtendedAPITestCase


class PerformIQCampaignListCreateTestCase(ExtendedAPITestCase):
    def _get_url(self, iq_campaign_id, export_type):
        url = reverse(Namespace.PERFORMIQ + ":" + PerformIQPathName.EXPORT, kwargs=dict(pk=iq_campaign_id)) \
              + f"?type={export_type}"
        return url

    def test_success_recommended(self):
        self.create_admin_user()
        export_filename = "test_recommended_export.csv"
        results = dict(
            exports={
                EXPORT_RESULTS_KEYS.RECOMMENDED_EXPORT_FILENAME: export_filename,
            }
        )
        iq_campaign = IQCampaign.objects.create(results=results)
        url = self._get_url(iq_campaign.id, export_type=0)
        with mock.patch.object(PerformS3Exporter, "generate_temporary_url", return_value=export_filename):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(response.data["download_url"], export_filename)

    def test_success_wastage(self):
        self.create_admin_user()
        export_filename = "test_wastage_export.csv"
        results = dict(
            exports={
                EXPORT_RESULTS_KEYS.WASTAGE_EXPORT_FILENAME: export_filename,
            }
        )
        iq_campaign = IQCampaign.objects.create(results=results)
        url = self._get_url(iq_campaign.id, export_type=1)
        with mock.patch.object(PerformS3Exporter, "generate_temporary_url", return_value=export_filename):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(response.data["download_url"], export_filename)

    def test_invalid_export_type(self):
        self.create_admin_user()
        url = self._get_url(0, export_type=3)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_invalid_iq_campaign(self):
        self.create_admin_user()
        url = self._get_url(0, export_type=0)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
