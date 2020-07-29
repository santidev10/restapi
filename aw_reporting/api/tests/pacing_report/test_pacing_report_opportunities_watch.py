from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.api.urls.names import Name
from aw_reporting.models import Opportunity
from saas.urls.namespaces import Namespace
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase
from utils.unittests.int_iterator import int_iterator


class PacingReportWatchOpportunitiesTestCase(APITestCase):
    def _get_url(self, op_id):
        url = reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.OPPORTUNITY_WATCH, kwargs=dict(pk=op_id))
        return url

    def test_watch_success(self):
        """ Test user can not watch more than max allowed """
        user = self.create_admin_user()
        op = Opportunity.objects.create(id=f"id_{next(int_iterator)}")
        self.assertEqual(user.opportunities.count(), 0)
        response = self.client.patch(self._get_url(op.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(user.opportunities.all().count(), 1)
        self.assertEqual(user.opportunities.first().id, op.id)

    def test_max_watch_opportunity(self):
        """ Test user can not watch more than max allowed """
        user = self.create_admin_user()
        ops = [Opportunity.objects.create(id=f"id_{next(int_iterator)}") for _ in range(6)]
        user.opportunities.add(*ops[:5])
        response = self.client.patch(self._get_url(ops[5].id))
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
