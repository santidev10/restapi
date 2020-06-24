from datetime import date
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from ads_analyzer.api.urls.names import AdsAnalyzerPathName
from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from aw_reporting.models import Opportunity
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import Permissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.s3_mock import mock_s3
from utils.unittests.test_case import ExtendedAPITestCase


class OpportunityTargetingRecipientBaseAPIViewTestCase(ExtendedAPITestCase):
    def _request(self, **query_params):
        url = reverse(AdsAnalyzerPathName.OPPORTUNITY_TARGETING_RECIPIENTS,
                      [Namespace.ADS_ANALYZER],
                      query_params=query_params)
        return self.client.get(url)


class OpportunityTargetingReportPermissions(OpportunityTargetingRecipientBaseAPIViewTestCase):
    def test_unauthorized(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_user_without_permissions(self):
        self.create_test_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_user_with_permissions(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_opportunity_report_recipients_list")

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_user_with_permissions_group(self):
        user = self.create_test_user()
        Permissions.sync_groups()
        user.add_custom_user_group(PermissionGroupNames.ADS_ANALYZER_RECIPIENTS)

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_allow_for_admin(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)


class OpportunityTargetingReportBehaviourAPIViewTestCase(OpportunityTargetingRecipientBaseAPIViewTestCase):
    def setUp(self) -> None:
        self.user = self.create_admin_user()

    def test_empty(self):
        response = self._request()

        self.assertEqual([], response.data)

    @mock_s3
    def test_structure(self):
        opportunity = Opportunity.objects.create(id=str(next(int_iterator)))
        date_from = date(2019, 1, 1)
        date_to = date(2019, 1, 2)
        test_user = get_user_model().objects.create(email="test@email.com", first_name="TestUser",
                                                    last_name="TestUser")
        with patch.object(post_save, "send"):
            report = OpportunityTargetingReport.objects.create(
                opportunity=opportunity,
                date_from=date_from,
                date_to=date_to,
                s3_file_key="example/report",
                status=ReportStatus.SUCCESS.value
            )

            report.recipients.add(test_user)

        response = self._request()

        self.assertEqual(1, len(response.data))
        self.assertEqual(
            {
                "id": test_user.id,
                "first_name": test_user.first_name,
                "last_name": test_user.last_name,
            },
            response.data[0]
        )
