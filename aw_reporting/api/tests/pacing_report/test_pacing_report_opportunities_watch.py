from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Opportunity
from cache.models import CacheItem
from dashboard.api.views import DashboardPacingAlertsAPIView
from dashboard.models import OpportunityWatch
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase
from utils.unittests.int_iterator import int_iterator


class PacingReportWatchOpportunitiesTestCase(APITestCase):
    def _get_url(self, op_id):
        url = reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.OPPORTUNITY_WATCH, kwargs=dict(pk=op_id))
        return url

    def _create_cache(self, user_id):
        dashboard_pacing_alert_cache_key = DashboardPacingAlertsAPIView.get_cache_key(user_id)
        CacheItem.objects.create(key=dashboard_pacing_alert_cache_key, value="stale")
        return dashboard_pacing_alert_cache_key

    def test_watch_success(self):
        user = self.create_admin_user()
        op = Opportunity.objects.create(id=f"id_{next(int_iterator)}")
        self.assertEqual(OpportunityWatch.objects.filter(user=user).count(), 0)

        dashboard_pacing_alert_cache_key = self._create_cache(user.id)

        response = self.client.patch(self._get_url(op.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(OpportunityWatch.objects.filter(user=user).count(), 1)
        self.assertEqual(OpportunityWatch.objects.filter(user=user).first().opportunity.id, op.id)
        self.assertFalse(CacheItem.objects.filter(key=dashboard_pacing_alert_cache_key).exists())

    def test_permissions_fail(self):
        """ Test user must have Tools group permissions """
        self.create_test_user()
        response = self.client.get(self._get_url(0))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_max_watch_opportunity(self):
        """ Test user can not watch more than max allowed """
        user = self.create_admin_user()
        ops = [Opportunity.objects.create(id=f"id_{next(int_iterator)}") for _ in range(6)]
        watches = [OpportunityWatch(user=user, opportunity=ops[i]) for i in range(len(ops))]
        OpportunityWatch.objects.bulk_create(watches)
        response = self.client.patch(self._get_url(ops[5].id))
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_delete_success(self):
        user = self.create_admin_user()
        op = Opportunity.objects.create(id=f"id_{next(int_iterator)}")
        OpportunityWatch.objects.create(user=user, opportunity=op)

        dashboard_pacing_alert_cache_key = self._create_cache(user.id)

        watched = OpportunityWatch.objects.filter(user=user)
        self.assertEqual(watched.count(), 1)
        self.assertEqual(watched[0].opportunity_id, op.id)

        response = self.client.delete(self._get_url(op.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(OpportunityWatch.objects.filter(user=user).count(), 0)
        self.assertFalse(CacheItem.objects.filter(key=dashboard_pacing_alert_cache_key).exists())