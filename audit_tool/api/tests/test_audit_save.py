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
        except Exception as e:
            raise KeyError("Failed to delete object. Object with key {} not found in bucket.".format(self.key))
        self.audit.delete()

    def test_audit_save_success(self):
        self.assertEqual(self.response.status_code, HTTP_200_OK)
        try:
            s3_object = self.s3.get_object(
                Bucket=AuditFileS3Exporter.bucket_name,
                Key=self.key
            )
        except Exception as e:
            raise KeyError("Object with key {} not found in bucket.".format(self.key))
        self.assertEqual(s3_object['ResponseMetadata']['HTTPStatusCode'], HTTP_200_OK)

    def test_seed_file_content_retrieval(self):
        try:
            f = AuditFileS3Exporter.get_s3_export_csv(self.key)
        except Exception as e:
            raise KeyError("Could not get csv from s3 for seed_file: {}.".format(self.audit.params['seed_file']))
        reader = csv.reader(f)
        for row in reader:
            self.assertEqual(row[0], "testing")
