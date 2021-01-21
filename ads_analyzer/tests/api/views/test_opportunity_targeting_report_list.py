from datetime import date
from unittest.mock import ANY
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from ads_analyzer.api.urls.names import AdsAnalyzerPathName
from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.models.opportunity_targeting_report import ReportStatus
from ads_analyzer.reports.opportunity_targeting_report.s3_exporter import OpportunityTargetingReportS3Exporter
from aw_reporting.models import Opportunity
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.s3_mock import mock_s3
from utils.unittests.test_case import ExtendedAPITestCase


class OpportunityTargetingReportBaseAPIViewTestCase(ExtendedAPITestCase):
    def _request(self, **kwargs):
        query_params = kwargs
        url = reverse(AdsAnalyzerPathName.OPPORTUNITY_TARGETING_REPORT, [Namespace.ADS_ANALYZER],
                      query_params=query_params)
        return self.client.get(url)


class OpportunityTargetingReportPermissions(OpportunityTargetingReportBaseAPIViewTestCase):
    def test_unauthorized(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_user_without_permissions(self):
        self.create_test_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_user_with_permissions(self):
        self.create_test_user(perms={
            StaticPermissions.ADS_ANALYZER: True
        })
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_allow_for_admin(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)


class OpportunityTargetingReportBehaviourAPIViewTestCase(OpportunityTargetingReportBaseAPIViewTestCase):
    def setUp(self) -> None:
        self.user = self.create_test_user()
        Permissions.sync_groups()
        self.user.add_custom_user_group(PermissionGroupNames.ADS_ANALYZER)

    def test_empty(self):
        response = self._request()

        self.assertEqual(
            {
                "current_page": 1,
                "items": [],
                "items_count": 0,
                "max_page": 1,
            },
            response.data
        )

    @mock_s3
    def test_structure(self):
        opportunity = Opportunity.objects.create(id=str(next(int_iterator)))
        date_from = date(2019, 1, 1)
        date_to = date(2019, 1, 2)
        test_user = get_user_model().objects.create(email="test1@email.com",
                                                    first_name="TestUser1", last_name="TestUser1")
        with patch.object(post_save, "send"):
            report = OpportunityTargetingReport.objects.create(
                opportunity=opportunity,
                date_from=date_from,
                date_to=date_to,
                s3_file_key="example/report",
                status=ReportStatus.SUCCESS.value
            )

            report.recipients.add(test_user)
            report.recipients.add(self.user)

        response = self._request()

        self.assertEqual(1, response.json()["items_count"])

        response_json_item = response.json()["items"][0]
        self.assertEqual(
            {
                "id": report.id,
                "opportunity_id": opportunity.id,
                "opportunity": opportunity.name,
                "date_from": report.date_from.isoformat(),
                "date_to": report.date_to.isoformat(),
                "created_at": report.created_at.isoformat().replace("+00:00", "Z"),
                "download_link": ANY,
                "recipients": ["TestUser1 TestUser1", "TestUser TestUser"],
                "status": report.status
            },
            response_json_item
        )

        self.assertEqual(
            OpportunityTargetingReportS3Exporter.generate_temporary_url(report.s3_file_key).split("?X-Amz-Algorithm")[
                0],
            response_json_item.get("download_link").split("?X-Amz-Algorithm")[0]
        )

    def test_empty_report_list(self):
        opportunity = Opportunity.objects.create(id=str(next(int_iterator)))
        date_from = date(2019, 1, 1)
        date_to = date(2019, 1, 2)
        with patch.object(post_save, "send"):
            OpportunityTargetingReport.objects.create(
                opportunity=opportunity,
                date_from=date_from,
                date_to=date_to,
                s3_file_key="example/report",
                status=ReportStatus.SUCCESS.value
            )

        response = self._request()

        self.assertEqual(0, response.json()["items_count"])

    @mock_s3
    def test_all_report_list(self):
        Permissions.sync_groups()
        self.user.add_custom_user_group(PermissionGroupNames.ADS_ANALYZER_RECIPIENTS)
        opportunity = Opportunity.objects.create(id=str(next(int_iterator)))
        date_from = date(2019, 1, 1)
        date_to = date(2019, 1, 2)
        with patch.object(post_save, "send"):
            OpportunityTargetingReport.objects.create(
                opportunity=opportunity,
                date_from=date_from,
                date_to=date_to,
                s3_file_key="example/report",
                status=ReportStatus.SUCCESS.value
            )

        response = self._request()

        self.assertEqual(1, response.json()["items_count"])

    @mock_s3
    def test_get_report_by_recipients(self):
        Permissions.sync_groups()
        self.user.add_custom_user_group(PermissionGroupNames.ADS_ANALYZER_RECIPIENTS)
        opportunity = Opportunity.objects.create(id=str(next(int_iterator)))
        date_from = date(2019, 1, 1)
        date_to = date(2019, 1, 2)
        test_user = get_user_model().objects.create(email="test2@email.com",
                                                    first_name="TestUser2", last_name="TestUser2")
        with patch.object(post_save, "send"):
            report = OpportunityTargetingReport.objects.create(
                opportunity=opportunity,
                date_from=date_from,
                date_to=date_to,
                s3_file_key="example/report",
                status=ReportStatus.SUCCESS.value
            )
            report.recipients.add(test_user)

        response = self._request(recipients=test_user.id)

        self.assertEqual(1, response.json()["items_count"])
