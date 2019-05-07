from unittest import mock

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from utils.utittests.test_case import ExtendedAPITestCase



class BrandSafetyChannelAPIViewTestCase(ExtendedAPITestCase):
    @mock.patch("brand_safety.api.views.brand_safety_channel.ElasticSearchConnector")
    def test_response_success(self, mock_connector):
        self.create_test_user()
        mock_connector.search_by_id.return_value = "test"
        url = reverse("brand_safety_urls:channel") + "1"
        response = self.client.get(url)
        print(response)

