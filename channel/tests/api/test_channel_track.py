from rest_framework.status import HTTP_200_OK

from channel.api.urls.names import ChannelPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class ChannelTrackTestCase(ExtendedAPITestCase, ESTestCase):
    url = reverse(ChannelPathName.CHANNEL_TRACK, [Namespace.CHANNEL])

    def setUp(self):


    def test_channel_track_works(self):