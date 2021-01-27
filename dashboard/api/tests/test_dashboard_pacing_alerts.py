import datetime
import json
from unittest import mock

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from cache.models import CacheItem
from dashboard.api.urls.names import DashboardPathName
from dashboard.api.views import DashboardPacingAlertsAPIView
from dashboard.models import OpportunityWatch
from aw_reporting.models import Opportunity
from aw_reporting.models import OpPlacement
from aw_reporting.models import Campaign
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase
from utils.datetime import now_in_default_tz


class DashboardPacingAlertTestCase(APITestCase):
    _url = reverse(DashboardPathName.DASHBOARD_PACING_ALERTS, [Namespace.DASHBOARD])

    def test_success(self):
        """ Test returns user watched opportunities sorted by name """
        user = self.create_test_user(perms={
            StaticPermissions.PACING_REPORT: True,
        })
        op1 = Opportunity.objects.create(name="first", id=f"id_{next(int_iterator)}", probability=100)
        pl1 = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name="p", opportunity=op1)
        Campaign.objects.create(name="c", salesforce_placement=pl1)

        op2 = Opportunity.objects.create(name="second", id=f"id_{next(int_iterator)}", probability=100)
        pl2 = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name="p", opportunity=op2)
        Campaign.objects.create(name="c", salesforce_placement=pl2)

        op3 = Opportunity.objects.create(id=f"id_{next(int_iterator)}", probability=100)
        pl3 = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name="p", opportunity=op3)
        Campaign.objects.create(name="c", salesforce_placement=pl3)

        ops = [op1, op2, op3]
        [OpportunityWatch.objects.create(user=user, opportunity=ops[i]) for i in range(2)]
        response = self.client.get(self._url)
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(data), 2)
        self.assertEqual(set(op["id"] for op in data), set(ops[i].id for i in range(2)))
        self.assertEqual(data[0]["name"], op2.name)
        self.assertEqual(data[1]["name"], op1.name)

    def test_refresh_cache(self):
        """ Test cache is refreshed after TTL expires """
        user = self.create_admin_user()
        op = Opportunity.objects.create(name="first", id=f"id_{next(int_iterator)}", probability=100)
        pl = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name="p", opportunity=op)
        Campaign.objects.create(name="c", salesforce_placement=pl)
        OpportunityWatch.objects.create(user=user, opportunity=op)

        # Mock that we are in the future to simulate ttl
        ttl_expired = now_in_default_tz() + datetime.timedelta(seconds=DashboardPacingAlertsAPIView.CACHE_TTL + 1)
        cache = CacheItem.objects.create(key="test_cache_key", value=json.dumps("stale_data"))

        with mock.patch("dashboard.api.views.dashboard_pacing_alerts.get_cache_key", return_value=cache.key),\
                mock.patch("dashboard.api.views.dashboard_pacing_alerts.now_in_default_tz", return_value=ttl_expired):
            response = self.client.get(self._url)
            data = response.data
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertNotEqual(cache.value, data)

            cache.refresh_from_db()
            self.assertEqual(json.loads(cache.value), data)

    def test_permissions_fail(self):
        """ Test user must have Tools group permissions """
        self.create_test_user()
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_default_none_watch(self):
        """ Test existing opportunities are returned if none are watched """
        self.create_admin_user()
        start = end = now_in_default_tz().date()
        op = Opportunity.objects.create(name="first", id=f"id_{next(int_iterator)}", probability=100,
                                        start=start, end=end)
        pl = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name="p", opportunity=op)
        Campaign.objects.create(name="c", salesforce_placement=pl)
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data[0]["id"], op.id)

    def test_watch_sort(self):
        """ Test watched opportunities with alerts should be sorted first """
        user = self.create_admin_user()
        start = end = now_in_default_tz().date()

        op1 = Opportunity.objects.create(name="first", id=f"id_{next(int_iterator)}", probability=100,
                                         start=start, end=end)
        pl1 = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name="p", opportunity=op1)
        Campaign.objects.create(name="c", salesforce_placement=pl1)

        op2 = Opportunity.objects.create(name="second", id=f"id_{next(int_iterator)}", probability=100,
                                         start=start, end=end, has_alerts=True)
        pl2 = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name="p", opportunity=op2)
        Campaign.objects.create(name="c", salesforce_placement=pl2)

        OpportunityWatch.objects.create(user=user, opportunity=op1)
        OpportunityWatch.objects.create(user=user, opportunity=op2)

        with mock.patch("aw_reporting.reports.pacing_report.is_opp_under_margin") as mock_under_margin:
            mock_under_margin.side_effect = [True, False]
            response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]["id"], op2.id)
        self.assertEqual(response.data[1]["id"], op1.id)
        self.assertTrue(response.data[0]["alerts"])
        self.assertFalse(response.data[1]["alerts"])
