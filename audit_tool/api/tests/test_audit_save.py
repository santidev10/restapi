from tempfile import mkstemp
import csv
from rest_framework.status import HTTP_200_OK

from audit_tool.models import AuditProcessor
from audit_tool.api.urls.names import AuditPathName
from audit_tool.api.views.audit_save import AuditFileS3Exporter
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class AuditSaveAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_SAVE, [Namespace.AUDIT_TOOL])

    def setUp(self):
        self.create_admin_user()
        self.s3 = AuditFileS3Exporter._s3()
        self.tmp_file = mkstemp(suffix=".csv")
        self.tmp_name = self.tmp_file[1]
        self.body = {}

    def test_audit_save_success(self):
        with open(self.tmp_name, 'w+', encoding='utf-8-sig') as f:
            f.write("testing")
            self.body['source_file'] = f
            response = self.client.post(self.url + "?name=test&audit_type=0&language=en/", self.body)
        self.assertEqual(response.status_code, HTTP_200_OK)
        audit = AuditProcessor.objects.get(id=response.data["id"])
        key = audit.params['seed_file']
        try:
            s3_object = self.s3.get_object(
                Bucket=AuditFileS3Exporter.bucket_name,
                Key=key
            )
        except Exception as e:
            raise KeyError("Object with key {} not found in bucket.".format(key))
        self.assertEqual(s3_object['ResponseMetadata']['HTTPStatusCode'], HTTP_200_OK)
        try:
            self.s3.delete_object(
                Bucket=AuditFileS3Exporter.bucket_name,
                Key=key
            )
        except Exception as e:
            raise KeyError("Failed to delete object. Object with key {} not found in bucket.".format(key))

    def test_seed_file_content_retrieval(self):
        with open(self.tmp_name, 'w+', encoding='utf-8-sig') as f:
            f.write("testing")
            self.body['source_file'] = f
            response = self.client.post(self.url + "?name=test&audit_type=0&language=en/", self.body)
        self.assertEqual(response.status_code, HTTP_200_OK)
        audit = AuditProcessor.objects.get(id=response.data["id"])
        key = audit.params['seed_file']
        try:
            f = AuditFileS3Exporter.get_s3_export_csv(key)
        except Exception as e:
            raise KeyError("Could not get csv from s3 for seed_file: {}.".format(audit.params['seed_file']))
        reader = csv.reader(f)
        for row in reader:
            self.assertEqual(row[0], "testing")
        try:
            self.s3.delete_object(
                Bucket=AuditFileS3Exporter.bucket_name,
                Key=key
            )
        except Exception as e:
            raise KeyError("Failed to delete object. Object with key {} not found in bucket.".format(key))
