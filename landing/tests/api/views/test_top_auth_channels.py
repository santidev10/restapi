from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED

from es_components.tests.utils import ESTestCase
from landing.api.names import LandingNames
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class ChannelListExportTestCase(ExtendedAPITestCase, ESTestCase):
    url = reverse(LandingNames.TOP_AUTH_CHANNELS, [Namespace.LANDING])

    def test_unauthorized(self):
        response = self.client.get(self.url)

        self.assertEqual(HTTP_401_UNAUTHORIZED, response.status_code)

    def test_allowed(self):
        self.create_test_user()

        response = self.client.get(self.url)

        self.assertEqual(HTTP_200_OK, response.status_code)
