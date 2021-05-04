import datetime
import json
from unittest import mock
from io import BytesIO

import boto3
from django.conf import settings
from django.urls import reverse
from moto import mock_s3
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_404_NOT_FOUND

from audit_tool.models import AuditProcessor
from oauth.constants import OAuthType
from oauth.models import AdGroup
from oauth.models import Campaign
from oauth.models import InsertionOrder
from oauth.models import LineItem
from oauth.models import DV360Partner
from oauth.models import DV360Advertiser
from saas.urls.namespaces import Namespace
from segment.api.urls.names import Name
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models.constants import Params
from segment.models.constants import SegmentTypeEnum
from segment.models.utils.generate_segment_utils import GenerateSegmentUtils
from utils.datetime import now_in_default_tz
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator


@mock_s3
class CTLDV360SyncTestCase(ExtendedAPITestCase):
    def _get_url(self, segment_id):
        return reverse(Namespace.SEGMENT_V2 + ":" + Name.SEGMENT_SYNC_DV360, kwargs=dict(pk=segment_id))

    def setUp(self) -> None:
        self.user = self.create_test_user()

    def _mock_data(self):
        segment = CustomSegment.objects.create(owner=self.user, segment_type=int(SegmentTypeEnum.CHANNEL))
        partner = DV360Partner.objects.create(id=next(int_iterator))
        advertiser = DV360Advertiser.objects.create(id=next(int_iterator), partner=partner)
        campaign = Campaign.objects.create(id=next(int_iterator), advertiser=advertiser, oauth_type=int(OAuthType.DV360))
        io = InsertionOrder.objects.create(id=next(int_iterator), campaign=campaign)
        line_item = LineItem.objects.create(id=next(int_iterator), insertion_order=io)
        adgroups = [AdGroup.objects.create(id=next(int_iterator), oauth_type=int(OAuthType.DV360), line_item=line_item)
                    for _ in range(2)]
        return segment, advertiser, adgroups

    def test_invalid_ctl(self):
        response = self.client.post(self._get_url(-1))
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_invalid_advertiser(self):
        segment, advertiser, adgroups = self._mock_data()
        payload = json.dumps(dict(advertiser_id=-1, adgroup_ids=[adgroups[0].id]))
        response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def _create_audit_with_dv360(self, advertiser_id, adgroup_ids):
        audit = AuditProcessor.objects.create(params={
            Params.DV360_SYNC_DATA: {
                Params.ADGROUP_IDS: adgroup_ids,
                Params.ADVERTISER_ID: advertiser_id
            }
        })
        return audit

    def test_invalid_adgroups(self):
        segment, advertiser, adgroups = self._mock_data()
        with self.subTest("None provided"):
            payload = json.dumps(dict(advertiser_id=advertiser.id, adgroup_ids=[]))
            response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

        with self.subTest("Some invalid"):
            payload = json.dumps(dict(advertiser_id=advertiser.id, adgroup_ids=[adgroups[0].id, -1]))
            response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

        with self.subTest("All invalid"):
            payload = json.dumps(dict(advertiser_id=advertiser.id, adgroup_ids=[-2, -1]))
            response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_success(self):
        mock_audit = AuditProcessor.objects.create()
        segment, advertiser, adgroups = self._mock_data()
        ag_ids = [ag.id for ag in adgroups]
        payload = json.dumps(dict(
            advertiser_id=advertiser.id,
            adgroup_ids=ag_ids,
        ))
        with mock.patch("segment.api.views.custom_segment.segment_dv360_sync.SegmentDV360SyncAPIView._start_audit", return_value=mock_audit) as mock_start_audit:
            response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        segment.refresh_from_db()
        mock_start_audit.assert_called_once()
        self.assertEqual(segment.params[Params.DV360_SYNC_DATA][Params.META_AUDIT_ID], mock_audit.id)

    def test_success_audit_creation(self):
        """ Test audit is created for DV360 SDF creation and source file for audit is uploaded """
        segment, advertiser, adgroups = self._mock_data()
        export_filename = "test.csv"
        export = CustomSegmentFileUpload.objects.create(segment=segment, filename=export_filename,
                                                        admin_filename=export_filename, query={})
        ag_ids = [ag.id for ag in adgroups]

        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)
        conn.create_bucket(Bucket=settings.AMAZON_S3_AUDITS_FILES_BUCKET_NAME)

        url = f"https://www.youtube.com/channel/{'test_channel'.zfill(24)}"
        file = BytesIO()
        file.write(b"URL\n")
        file.write(url.encode("utf-8"))
        file.write(b"\n")
        file.seek(0)
        conn.Object(segment.s3.bucket_name, export.filename).upload_fileobj(file)
        payload = json.dumps(dict(
            advertiser_id=advertiser.id,
            adgroup_ids=ag_ids,
        ))
        response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        audit = AuditProcessor.objects.get(params__segment_id=segment.id)
        audit_dv360_params = audit.params[Params.DV360_SYNC_DATA]
        self.assertEqual(audit.params["segment_id"], segment.id)
        self.assertEqual(audit_dv360_params[Params.ADVERTISER_ID], advertiser.id)
        self.assertEqual(audit_dv360_params[Params.ADGROUP_IDS], ag_ids)

        # Check audit seed file was uploaded correctly
        audit_seed_data = conn.Object(settings.AMAZON_S3_AUDITS_FILES_BUCKET_NAME, audit.params["seed_file"]).get()["Body"]
        rows = [row.decode("utf-8") for row in audit_seed_data]
        self.assertEqual(rows[0].strip(), url)

    def test_ctl_export_not_found(self):
        """ Test handling if CTL does not have an export to generate an SDF for. Should not create audit s"""
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME)

        segment, advertiser, adgroups = self._mock_data()
        ag_ids = [ag.id for ag in adgroups]
        payload = json.dumps(dict(
            advertiser_id=advertiser.id,
            adgroup_ids=ag_ids,
        ))
        with mock.patch.object(GenerateSegmentUtils, "start_audit") as mock_start_audit:
            response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)
        self.assertTrue("Export not found" in response.data["detail"])
        # Audit should not be created if ctl has no export
        self.assertFalse(AuditProcessor.objects.filter(params__segment_id=segment.id).exists())
        mock_start_audit.assert_not_called()

    @mock.patch("segment.api.views.custom_segment.segment_dv360_sync.SegmentDV360SyncAPIView._download_segment_export",
                return_value="test.csv")
    def test_reuse_audit_check(self, mock_download_segment_export):
        """ Test logic determining if existing AuditProcessor should be used """
        with self.subTest("Audit should be created if SDF export requested for first time"):
            segment, advertiser, adgroups = self._mock_data()
            ag_ids = [ag.id for ag in adgroups]
            payload = json.dumps(dict(
                advertiser_id=advertiser.id,
                adgroup_ids=ag_ids,
            ))
            with mock.patch.object(GenerateSegmentUtils, "start_audit") as mock_start_audit:
                response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_200_OK)
            mock_start_audit.assert_called_once()

        with self.subTest("Audit should be created if existing AuditProcessor expires"):
            segment, advertiser, adgroups = self._mock_data()
            ag_ids = [ag.id for ag in adgroups]

            audit = self._create_audit_with_dv360(advertiser.id, ag_ids)
            # Set audit to be invalid by expiring it
            outdated = datetime.datetime.now() - datetime.timedelta(hours=settings.AUDIT_SDF_VALID_TIME + 1)
            AuditProcessor.objects.filter(id=audit.id).update(created=outdated)
            segment.update_params(audit.id, Params.DV360_SYNC_DATA, data_field=Params.META_AUDIT_ID, save=True)
            ag_ids = [ag.id for ag in adgroups]
            payload = json.dumps(dict(
                advertiser_id=advertiser.id,
                adgroup_ids=ag_ids,
            ))
            now = now_in_default_tz()
            # Mock now_in_default_tz to ensure that the audit created timestamp is being compared
            with mock.patch.object(GenerateSegmentUtils, "start_audit") as mock_start_audit,\
                    mock.patch("segment.api.views.custom_segment.segment_dv360_sync.now_in_default_tz", return_value=now) as mock_now:
                response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_200_OK)
            mock_start_audit.assert_called_once()
            mock_now.assert_called_once()

        with self.subTest("Audit should be reused if existing AuditProcessor is valid"):
            segment, advertiser, adgroups = self._mock_data()
            ag_ids = [ag.id for ag in adgroups]

            audit = self._create_audit_with_dv360(advertiser.id, ag_ids)
            segment.update_params(audit.id, Params.DV360_SYNC_DATA, data_field=Params.META_AUDIT_ID, save=True)
            with mock.patch.object(GenerateSegmentUtils, "start_audit") as mock_start_audit,\
                    mock.patch("segment.api.views.custom_segment.segment_dv360_sync.generate_sdf_segment_task.delay") as mock_generate_sdf:
                response = self.client.post(self._get_url(segment.id), payload, content_type="application/json")
                self.assertEqual(response.status_code, HTTP_200_OK)
            mock_generate_sdf.assert_called_once()
            mock_start_audit.assert_not_called()
