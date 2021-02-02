from unittest import mock

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.api.urls.names import Name
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.reverse import reverse
from utils.unittests.s3_mock import mock_s3
from utils.unittests.test_case import ExtendedAPITestCase

EXPECT_MESSSAGE = "Report is in queue for preparing. Task position in queue is 1. After it is finished exporting, " \
                  "you will receive message via email."

ERROR_MESSAGE = "User emails are not defined"

TEST_EMAIL = "test@test.test"


class PacingReportCollectTestCase(ExtendedAPITestCase):
    url = reverse(Name.PacingReport.COLLECT, [Namespace.AW_REPORTING])

    def setUp(self) -> None:
        self.user = self.create_test_user(perms={StaticPermissions.PACING_REPORT: True})

    @mock_s3
    @mock.patch("utils.celery.utils.get_queue_size", return_value=0)
    def test_success(self, *args, **kwargs):
        response = self.client.get("{}?emails={}".format(self.url, TEST_EMAIL))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(EXPECT_MESSSAGE, response.data.get("message"))

    @mock_s3
    @mock.patch("utils.celery.utils.get_queue_size", return_value=0)
    def test_success_with_filters(self, *args, **kwargs):
        response = self.client.get("{}?period=this_month&status=active&emails={}".format(self.url, TEST_EMAIL))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(EXPECT_MESSSAGE, response.data.get("message"))

    @mock_s3
    @mock.patch("utils.celery.utils.get_queue_size", return_value=0)
    def test_error(self, *args, **kwargs):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(ERROR_MESSAGE, response.data)
