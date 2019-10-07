import json
from datetime import date
from datetime import datetime
from datetime import timedelta
from unittest.mock import ANY

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from ads_analyzer.api.urls.names import AdsAnalyzerPathName
from ads_analyzer.models.opportunity_targeting_report import OpportunityTargetingReport
from aw_reporting.models import Opportunity
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase

from utils.utittests.patch_now import patch_now


class OpportunityTargetingReportBaseAPIViewTestCase(ExtendedAPITestCase):
    def _request(self, data=None):
        url = reverse(AdsAnalyzerPathName.OPPORTUNITY_TARGETING_REPORT, [Namespace.ADS_ANALYZER])
        data = data or dict()
        return self.client.post(url, json.dumps(data, default=str), content_type="application/json")


class OpportunityTargetingReportPermissions(OpportunityTargetingReportBaseAPIViewTestCase):
    def _request(self, *args, **kwargs):
        data = dict(
            opportunity=Opportunity.objects.create(id=next(int_iterator)).id,
            date_from="2019-01-01",
            date_to="2019-01-01",
        )
        return super()._request(data)

    def test_unauthorized(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_user_without_permissions(self):
        self.create_test_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_user_with_permissions(self):
        user = self.create_test_user()
        user.add_custom_user_permission("create_opportunity_report")

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_allow_for_admin(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)


class OpportunityTargetingReportBehaviourAPIViewTestCase(OpportunityTargetingReportBaseAPIViewTestCase):
    def setUp(self) -> None:
        self.create_admin_user()

    def test_validate_required(self):
        required_fields = "opportunity"

        response = self._request(dict())

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertIn(required_fields, response.data)

    def test_invalid_opportunity(self):
        response = self._request(dict(
            opportunity="missed_id",
            date_from="2019-01-01",
            date_to="2019-01-01",
        ))

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_create_report_entity(self):
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        date_from, date_to = date(2019, 1, 2), date(2019, 1, 3)
        reports_queryset = OpportunityTargetingReport.objects.filter(
            opportunity=opportunity,
            date_from=date_from,
            date_to=date_to,
        )
        self.assertEqual(0, reports_queryset.count())

        response = self._request(dict(
            opportunity=opportunity.id,
            date_from=date_from,
            date_to=date_to,
        ))

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(1, reports_queryset.count())

    def test_create_report_recipients(self):
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        date_from, date_to = date(2019, 1, 2), date(2019, 1, 3)

        self._request(dict(
            opportunity=opportunity.id,
            date_from=date_from,
            date_to=date_to,
        ))

        report = OpportunityTargetingReport.objects.get(opportunity=opportunity)
        self.assertEqual(1, report.recipients.count())

    def test_report_does_not_exist(self):
        opportunity = Opportunity.objects.create(id=next(int_iterator), name="Opportunity #123")
        date_from, date_to = date(2019, 1, 2), date(2019, 1, 3)

        response = self._request(dict(
            opportunity=opportunity.id,
            date_from=date_from,
            date_to=date_to,
        ))

        self.assertEqual(
            dict(
                status="created",
                message="Processing.  You will receive an email when your export is ready."
            ),
            response.json()
        )

    def test_report_exists_in_progress(self):
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        date_from, date_to = date(2019, 1, 2), date(2019, 1, 3)
        OpportunityTargetingReport.objects.create(
            opportunity=opportunity,
            date_from=date_from,
            date_to=date_to,
            external_link=None
        )

        response = self._request(dict(
            opportunity=opportunity.id,
            date_from=date_from,
            date_to=date_to,
        ))

        self.assertEqual(
            dict(
                status="created",
                message="Processing.  You will receive an email when your export is ready.",
            ),
            response.json()
        )

    def test_report_exists_and_ready(self):
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        date_from, date_to = date(2019, 1, 2), date(2019, 1, 3)
        external_link = "http://some_url.com"
        OpportunityTargetingReport.objects.create(
            opportunity=opportunity,
            date_from=date_from,
            date_to=date_to,
            external_link=external_link
        )

        response = self._request(dict(
            opportunity=opportunity.id,
            date_from=date_from,
            date_to=date_to,
        ))

        self.assertEqual(
            dict(
                status="ready",
                report_link=external_link,
                message=ANY,
            ),
            response.json()
        )

    def test_report_expire(self):
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        date_from, date_to = date(2019, 1, 2), date(2019, 1, 3)
        OpportunityTargetingReport.objects.create(
            opportunity=opportunity,
            date_from=date_from,
            date_to=date_to
        )

        with patch_now(datetime.now() + timedelta(hours=25)):

            response = self._request(dict(
                opportunity=opportunity.id,
                date_from=date_from,
                date_to=date_to,
            ))

        self.assertEqual(
            dict(
                status="created",
                message=ANY,
            ),
            response.json()
        )
