import json
import datetime

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND

from audit_tool.models import AuditProcessor

from audit_tool.api.urls.names import AuditPathName
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class AuditListAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_LIST, [Namespace.AUDIT_TOOL])
    def test_success_get(self):
        self.create_admin_user()
        now = datetime.datetime.now()
        new_audit_1 = AuditProcessor.objects.create(
            created=now,
            started=now,
            updated=now + datetime.timedelta(days=5),
            completed=now + datetime.timedelta(days=6),
            pause=5,
        )

        new_audit_2 = AuditProcessor.objects.create(
            created=now,
            started=now,
            updated=now + datetime.timedelta(days=1),
            completed=now + datetime.timedelta(days=2),
            pause=3,
        )

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)