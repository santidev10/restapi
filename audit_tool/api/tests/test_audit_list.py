import json

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND

from audit_tool.api.urls.names import AuditPathName
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class AuditListAPITestCase(ExtendedAPITestCase):
    url = reverse(AuditPathName.AUDIT_LIST, [Namespace.AUDIT_TOOL])
