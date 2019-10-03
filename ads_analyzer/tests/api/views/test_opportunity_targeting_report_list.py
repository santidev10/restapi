from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from ads_analyzer.api.urls.names import AdsAnalyzerPathName
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class OpportunityTargetingReportBaseAPIViewTestCase(ExtendedAPITestCase):
    def _request(self, **query_params):
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
