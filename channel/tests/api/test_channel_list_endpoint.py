from mock import patch
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND

from channel.api.urls.names import ChannelPathName
from saas.urls.namespaces import Namespace
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import SegmentFunctionalityMixin
from utils.utils_tests import SingleDatabaseApiConnectorPatcher
from utils.utils_tests import reverse


class ChannelListTestCase(ExtendedAPITestCase, SegmentFunctionalityMixin):
    url = reverse(ChannelPathName.CHANNEL_LIST, [Namespace.CHANNEL])

    def test_channel_segment_filter_does_not_exists(self):
        self.create_admin_user()
        url = "{}?channel_segment=1".format(self.url)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_video_segment_filter_does_not_exists(self):
        self.create_admin_user()
        url = "{}?video_segment=1".format(self.url)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_video_segment_and_channel_segment_together_using(self):
        self.create_admin_user()
        url = "{}?video_segment=1&channel_segment=1".format(self.url)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data.keys(), {"error"})

    def test_simple_list_works(self):
        self.create_admin_user()
        with patch("channel.api.views.channel_list.Connector", new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
