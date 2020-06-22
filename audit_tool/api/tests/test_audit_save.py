import csv
import json
from tempfile import mkstemp
from uuid import uuid4

from mock import patch
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.api.urls.names import AuditPathName
from audit_tool.api.views.audit_save import AuditFileS3Exporter
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditProcessor
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class AuditSaveAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_SAVE, [Namespace.AUDIT_TOOL])
    custom_segment_model = None
    custom_segment_export_model = None

    def setUp(self):
        # Import and set models to avoid recursive ImportError
        from segment.models import CustomSegment
        from segment.models import CustomSegmentFileUpload
        self.custom_segment_model = CustomSegment
        self.custom_segment_export_model = CustomSegmentFileUpload
        self.create_admin_user()
        self.s3 = AuditFileS3Exporter._s3()
        self.tmp_file = mkstemp(suffix=".csv")
        self.tmp_name = self.tmp_file[1]
        self.body = {}
        with open(self.tmp_name, 'w+', encoding='utf-8-sig') as f:
            f.write("testing")
            self.body['source_file'] = f
            self.response = self.client.post(self.url + "?name=test&audit_type=0&language=en/", self.body)
        self.audit = AuditProcessor.objects.get(id=self.response.data["id"])
        self.key = self.audit.params['seed_file']

    def tearDown(self):
        try:
            self.s3.delete_object(
                Bucket=AuditFileS3Exporter.bucket_name,
                Key=self.key
            )
        except Exception:
            raise KeyError("Failed to delete object. Object with key {} not found in bucket.".format(self.key))
        self.audit.delete()

    def test_audit_save_success(self):
        self.assertEqual(self.response.status_code, HTTP_200_OK)
        try:
            s3_object = self.s3.get_object(
                Bucket=AuditFileS3Exporter.bucket_name,
                Key=self.key
            )
        except Exception:
            raise KeyError("Object with key {} not found in bucket.".format(self.key))
        self.assertEqual(s3_object['ResponseMetadata']['HTTPStatusCode'], HTTP_200_OK)

    def test_seed_file_content_retrieval(self):
        try:
            f = AuditFileS3Exporter.get_s3_export_csv(self.key)
        except Exception:
            raise KeyError("Could not get csv from s3 for seed_file: {}.".format(self.audit.params['seed_file']))
        reader = csv.reader(f)
        for row in reader:
            self.assertEqual(row[0], "testing")

    def test_reject_permission(self):
        """ Users must have userprofile.audit_vet_admin permission """
        user = self.create_test_user()
        segment = self.custom_segment_model.objects.create(uuid=uuid4(), owner=user, title="test", segment_type=0,
                                                           list_type=0)
        params = {"segment_id": segment.id, }
        response = self.client.patch(self.url, data=params)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_enable_vetting_creates_audit(self):
        """ Saving audit instructions for the first time should create audit and vetting items """
        user = self.create_test_user()
        user.add_custom_user_group(PermissionGroupNames.AUDIT_VIEW)
        user.add_custom_user_group(PermissionGroupNames.AUDIT_VET_ADMIN)
        segment = self.custom_segment_model.objects.create(
            owner=user, title="test", segment_type=0, list_type=0, statistics={"items_count": 1}, uuid=uuid4(),
        )
        self.custom_segment_export_model.objects.create(segment=segment, query={})
        data = {
            "segment_id": segment.id,
            "instructions": "test_instructions"
        }
        with patch("audit_tool.api.views.audit_save.generate_audit_items") as mock_generate:
            response = self.client.patch(self.url, json.dumps(data), content_type="application/json")
        segment.refresh_from_db()
        audit = AuditProcessor.objects.get(id=segment.audit_id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["instructions"], data["instructions"])
        self.assertEqual(audit.params["instructions"], data["instructions"])
        mock_generate.delay.assert_called_once()

    def test_update_audit_does_not_create(self):
        """ Updating audit instructions should not create new audit nor audit vetting items """
        user = self.create_test_user()
        user.add_custom_user_group(PermissionGroupNames.AUDIT_VIEW)
        user.add_custom_user_group(PermissionGroupNames.AUDIT_VET_ADMIN)
        audit = AuditProcessor.objects.create(source=1, audit_type=2, params=dict(instructions="old instructions"))
        segment = self.custom_segment_model.objects.create(
            owner=user, title="test", segment_type=1, list_type=0,
            statistics={"items_count": 1}, uuid=uuid4(), audit_id=audit.id
        )
        self.custom_segment_export_model.objects.create(segment=segment, query={})
        data = {
            "audit_id": audit.id,
            "instructions": "new instructions"
        }
        with patch("audit_tool.api.views.audit_save.generate_audit_items") as mock_generate:
            response = self.client.patch(self.url, json.dumps(data), content_type="application/json")
        segment.refresh_from_db()
        audit.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["instructions"], data["instructions"])
        self.assertEqual(audit.params["instructions"], data["instructions"])
        self.assertFalse(mock_generate.delay.called)
        self.assertFalse(AuditChannelVet.objects.all())

    def test_reject_parameters(self):
        self.create_admin_user()
        data = {
            "segment_id": 1
        }
        response = self.client.patch(self.url, json.dumps(data), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
