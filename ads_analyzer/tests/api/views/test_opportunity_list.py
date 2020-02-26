from datetime import date
from datetime import datetime
from datetime import timedelta

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from ads_analyzer.api.urls.names import AdsAnalyzerPathName
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import Permissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class OpportunityTargetingReportBaseAPIViewTestCase(ExtendedAPITestCase):
    def _request(self):
        url = reverse(AdsAnalyzerPathName.OPPORTUNITY_LIST, [Namespace.ADS_ANALYZER])
        return self.client.get(url, content_type="application/json")


class OpportunityListPermissionsAPIViewTestCase(OpportunityTargetingReportBaseAPIViewTestCase):
    def test_unauthorized(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_user_without_permissions(self):
        self.create_test_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_user_with_permissions(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_opportunity_list")

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_user_with_permissions_group(self):
        user = self.create_test_user()
        Permissions.sync_groups()
        user.add_custom_user_group(PermissionGroupNames.ADS_ANALYZER)

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_allow_for_admin(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)


class OpportunityTargetingReportAPIViewTestCase(OpportunityTargetingReportBaseAPIViewTestCase):
    def setUp(self) -> None:
        self.create_admin_user()

    def test_empty(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual([], response.json())

    def test_single(self):
        start_date = date(datetime.now().year - 1, 12, 1)
        opportunity = Opportunity.objects.create(
            id=next(int_iterator),
            name="Test Opportunity",
            start=start_date
        )
        opportunity.refresh_from_db()
        placement = OpPlacement.objects.create(
            id=next(int_iterator),
            name="Test Placement",
            opportunity=opportunity)
        placement.refresh_from_db()
        Campaign.objects.create(
            salesforce_placement=placement,
            status="eligible",
            start_date=start_date
        )
        response = self._request()

        self.assertEqual([dict(
            id=opportunity.id,
            name=opportunity.name,
            start="{}-12-01".format(datetime.now().year - 1),
        )], response.json())

    def test_no_active_opportunity(self):
        Opportunity.objects.create(
            id=next(int_iterator),
            name="Test Opportunity"
        )

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual([], response.json())

    def test_no_last_years_opportunity(self):
        start_date = date(datetime.now().year - 5, 12, 1)
        opportunity = Opportunity.objects.create(
            id=next(int_iterator),
            name="Test Opportunity"
        )
        opportunity.refresh_from_db()
        placement = OpPlacement.objects.create(
            id=next(int_iterator),
            name="Test Placement",
            opportunity=opportunity)
        placement.refresh_from_db()
        Campaign.objects.create(
            salesforce_placement=placement,
            status="eligible",
            start_date=start_date
        )

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual([], response.json())

    def test_no_future_opportunity(self):
        Opportunity.objects.create(
            id=next(int_iterator),
            name="Test Opportunity",
            start=datetime.now() + timedelta(days=1)
        )
        Opportunity.objects.create(
            id=next(int_iterator),
            name="Test Opportunity",
            start=datetime.now() - timedelta(days=1)
        )

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(1, len(response.json()))
