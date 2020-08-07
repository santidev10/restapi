from datetime import timedelta
import mock

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
from userprofile.permissions import Permissions
from userprofile.permissions import PermissionGroupNames
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase
from utils.datetime import now_in_default_tz


class DashboardPacingAlertTestCase(APITestCase):
    _url = reverse(DashboardPathName.DASHBOARD_PACING_ALERTS, [Namespace.DASHBOARD])

    def test_success(self):
        """ Test returns user watched opportunities sorted by name """
        Permissions.sync_groups()
        user = self.create_test_user()
        user.add_custom_user_group(PermissionGroupNames.TOOLS)
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
        self.assertEqual(data[0]["name"], op1.name)
        self.assertEqual(data[1]["name"], op2.name)

    def test_refresh_cache(self):
        """ Test cache is refreshed after TTL expires """
        user = self.create_admin_user()
        op = Opportunity.objects.create(name="first", id=f"id_{next(int_iterator)}", probability=100)
        pl = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name="p", opportunity=op)
        Campaign.objects.create(name="c", salesforce_placement=pl)
        OpportunityWatch.objects.create(user=user, opportunity=op)

        # Mock that we are in the future to simulate ttl
        ttl_expired = now_in_default_tz() + timedelta(seconds=DashboardPacingAlertsAPIView.CACHE_TTL + 1)
        cache = CacheItem.objects.create(key="test_cache_key", value="stale_data")

        with mock.patch("dashboard.api.views.dashboard_pacing_alerts.get_cache_key", return_value=cache.key),\
                mock.patch("dashboard.api.views.dashboard_pacing_alerts.now_in_default_tz", return_value=ttl_expired):
            response = self.client.get(self._url)
            data = response.data
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertNotEqual(cache.value, data)

            cache.refresh_from_db()
            self.assertEqual(cache.value, data)

    def test_permissions_fail(self):
        """ Test user must have Tools group permissions """
        self.create_test_user()
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_default_none_watch(self):
        """ Test existing opportunities are returned in none are watched """
        self.create_admin_user()
        op = Opportunity.objects.create(name="first", id=f"id_{next(int_iterator)}", probability=100)
        pl = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name="p", opportunity=op)
        Campaign.objects.create(name="c", salesforce_placement=pl)
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data[0]["id"], op.id)
