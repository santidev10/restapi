from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED

from channel.api.urls.names import ChannelPathName
from saas.urls.namespaces import Namespace
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class LanguagesEndpointTestCase(ExtendedAPITestCase):
    url = reverse(ChannelPathName.LANGUAGES_LIST, [Namespace.CHANNEL])

    def test_languages_authentication_required(self):
        """
        test that anon users cannot access the languages endpoint
        """
        unauthorized_response = self.client.get(self.url)
        self.assertEqual(unauthorized_response.status_code, HTTP_401_UNAUTHORIZED)
        self.create_test_user()
        authorized_response = self.client.get(self.url)
        self.assertEqual(authorized_response.status_code, HTTP_200_OK)
