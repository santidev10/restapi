from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from ads_analyzer.api.urls.names import AdsAnalyzerPathName
from aw_reporting.models import Opportunity
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


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
        opportunity = Opportunity.objects.create(
            id=next(int_iterator),
            name="Test Opportunity"
        )
        opportunity.refresh_from_db()
        response = self._request()

        self.assertEqual([dict(
            id=opportunity.id,
            name=opportunity.name,
        )], response.json())
