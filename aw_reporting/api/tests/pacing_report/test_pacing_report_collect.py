from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from saas.urls.namespaces import Namespace
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.s3_mock import mock_s3
from utils.utittests.reverse import reverse

EXPECT_MESSSAGE = "Report is in queue for preparing. Task position in queue is 1. After it is finished exporting, " \
                  "you will receive message via email and You might download it using " \
                  "following link"


class PacingReportCollectTestCase(ExtendedAPITestCase):
    url = reverse(Name.PacingReport.COLLECT, [Namespace.AW_REPORTING])

    @mock_s3
    def test_success(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(EXPECT_MESSSAGE, response.data.get("message"))

    @mock_s3
    def test_success_with_filters(self):
        response = self.client.get("{}?period=this_month&status=active".format(self.url))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(EXPECT_MESSSAGE, response.data.get("message"))
