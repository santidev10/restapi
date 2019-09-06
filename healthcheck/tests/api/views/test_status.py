from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_500_INTERNAL_SERVER_ERROR

from healthcheck.api.urls.names import HealthcheckPathName
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class StatusApiViewTestCase(ExtendedAPITestCase):
    def test_success_unauthorized(self):
        url = reverse(HealthcheckPathName.STATUS, [Namespace.HEALTHCHECK])

        response = self.client.get(url)

        self.assertEqual(HTTP_200_OK, response.status_code)

    def test_echo_500(self):
        url = reverse(HealthcheckPathName.STATUS, [Namespace.HEALTHCHECK],
                      query_params=dict(echo_status=HTTP_500_INTERNAL_SERVER_ERROR))

        response = self.client.get(url)

        self.assertEqual(HTTP_500_INTERNAL_SERVER_ERROR, response.status_code)
