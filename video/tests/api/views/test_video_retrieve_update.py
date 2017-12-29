import json
from unittest.mock import patch

from rest_framework.reverse import reverse
from rest_framework.status import HTTP_200_OK

from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher


class VideoRetrieveUpdateTestSpec(ExtendedAPITestCase):
    def test_professional_user_should_see_video_aw_data(self):
        """
        Ticket https://channelfactory.atlassian.net/browse/SAAS-1695
        """
        user = self.create_test_user(True)
        user.set_permissions_from_plan('professional')
        user.save()

        with open('saas/fixtures/singledb_video_list.json') as data_file:
            data = json.load(data_file)
        video_id = data["items"][0]["id"]

        url = reverse("video_api_urls:video",
                      args=(video_id,))
        with patch("video.api.views.Connector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("aw_data", response.data)
