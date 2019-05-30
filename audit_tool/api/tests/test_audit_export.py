from tempfile import mkstemp
import csv
from rest_framework.status import HTTP_200_OK

from audit_tool.models import AuditProcessor
from audit_tool.api.urls.names import AuditPathName
from audit_tool.api.views.audit_export import AuditS3Exporter
from audit_tool.api.views import AuditExportApiView
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase

class AuditSaveAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_EXPORT, [Namespace.AUDIT_TOOL])

    def setUp(self):
        self.create_admin_user()
        video_audit = AuditProcessor.objects.create(audit_type=1, )
        channel_audit = AuditProcessor.objects.create(audit_type=2)
