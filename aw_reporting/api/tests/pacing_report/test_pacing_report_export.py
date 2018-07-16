from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from saas.urls.namespaces import Namespace
from utils.utils_tests import ExtendedAPITestCase


class PacingReportExportTestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.EXPORT)

    def test_success(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
