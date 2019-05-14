from datetime import datetime

from rest_framework.status import HTTP_200_OK

from audit_tool.models import AuditProcessor

from audit_tool.api.urls.names import AuditPathName
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class AuditListAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_LIST, [Namespace.AUDIT_TOOL])

    def setUp(self):
        self.create_admin_user()
        audit_1 = AuditProcessor.objects.create(audit_type=0)
        audit_2 = AuditProcessor.objects.create(audit_type=1, completed=datetime.now())
        audit_2 = AuditProcessor.objects.create(audit_type=2)
        audit_3 = AuditProcessor.objects.create(audit_type=2, completed=datetime.now())
        audit_3 = AuditProcessor.objects.create(audit_type=2, completed=datetime.now())

    def test_success_get(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 5)

    def test_success_get_filter_running(self):
        running_url = "{}?running=true".format(self.url)
        response = self.client.get(running_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

    def test_success_get_filter_completed(self):
        completed_url = "{}?running=false".format(self.url)
        response = self.client.get(completed_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 3)

    def test_success_get_filter_type(self):
        type_url = "{}?audit_type=2".format(self.url)
        response = self.client.get(type_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 3)