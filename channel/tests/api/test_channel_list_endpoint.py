from unittest.mock import patch

from rest_framework.status import HTTP_200_OK

from channel.api.urls.names import ChannelPathName
from saas.urls.namespaces import Namespace
from utils.utittests.reverse import reverse
from utils.utittests.segment_functionality_mixin import SegmentFunctionalityMixin
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.es_components_patcher import SearchDSLPatcher


class ChannelListTestCase(ExtendedAPITestCase, SegmentFunctionalityMixin):
    url = reverse(ChannelPathName.CHANNEL_LIST, [Namespace.CHANNEL])

    def test_simple_list_works(self):
        with patch("es_components.managers.channel.ChannelManager.search",
                   return_value=SearchDSLPatcher()):
            self.create_admin_user()
            response = self.client.get(self.url)
            self.assertEqual(response.status_code, HTTP_200_OK)
