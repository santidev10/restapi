from unittest import mock

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from saas.urls.namespaces import Namespace
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.s3_mock import mock_s3

EXPECT_MESSSAGE = "Report is in queue for preparing. After it is finished exporting, " \
                  "you will receive message via email and You might download it using "


class PacingReportExportTestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.COLLECT)

    @mock_s3
    def test_success(self):
        with mock.patch("aw_reporting.reports.pacing_report.PacingReport.get_opportunities", return_value=[]):

            response = self.client.get(self.url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertIn(EXPECT_MESSSAGE, response.data.get('message'))

    @mock_s3
    def test_success_with_filters(self):
        response = self.client.get("{}?period=this_month&status=active".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn(EXPECT_MESSSAGE, response.data.get('message'))
