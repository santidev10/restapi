import urllib
from urllib.parse import urlencode

from datetime import datetime

from rest_framework.status import HTTP_200_OK

from audit_tool.api.urls.names import AuditPathName
from audit_tool.models import AuditProcessor
from saas.urls.namespaces import Namespace
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class AuditListAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_LIST, [Namespace.AUDIT_TOOL])
    first_item_id = None

    def setUp(self):
        self.create_admin_user()
        first_item = AuditProcessor.objects.create(audit_type=0)
        self.first_item_id = first_item.id
        AuditProcessor.objects.create(audit_type=1, completed=datetime.now())
        AuditProcessor.objects.create(audit_type=2)
        AuditProcessor.objects.create(audit_type=2, completed=datetime.now())
        AuditProcessor.objects.create(audit_type=2, completed=datetime.now())

    def test_success_get(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['audits']['items_count'], 5)

    def test_success_get_filter_running(self):
        running_url = "{}?running=true".format(self.url)
        response = self.client.get(running_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['audits']['items_count'], 2)

    def test_success_get_filter_completed(self):
        completed_url = "{}?running=false".format(self.url)
        response = self.client.get(completed_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['audits']['items_count'], 3)

    def test_success_get_filter_type(self):
        type_url = "{}?audit_type=2".format(self.url)
        response = self.client.get(type_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['audits']['items_count'], 3)

    def test_get_filter_by_single_id(self):
        self.create_admin_user()
        items_to_filter = 1

        url = self.url + "?" + urllib.parse.urlencode({"audit_id": str(self.first_item_id)})
        response = self.client.get(url)

        self.assertEqual(len(response.data['audits']), items_to_filter)
        self.assertEqual(str(response.data["audits"][0]["id"]), str(self.first_item_id))
