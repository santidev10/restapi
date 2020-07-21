from django.contrib.auth.models import Group

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
    manager = ChannelManager(sections=(Sections.MAIN, Sections.CUSTOM_PROPERTIES))

    def setUp(self):
        self.channel_id_1 = str(next(int_iterator))
        channel_1 = Channel(**{
            "meta": {
                "id": self.channel_id_1
            }
        })
        self.manager.upsert([channel_1])

    def test_channel_track_simple(self):
        user = self.create_test_user()
        Group.objects.get_or_create(name=PermissionGroupNames.AUDIT_VET)
        user.add_custom_user_permission("vet_audit")
        user.add_custom_user_group(PermissionGroupNames.AUDIT_VET)
        channel_id_2 = str(next(int_iterator))
        channel_id_3 = str(next(int_iterator))
        channel_id_4 = str(next(int_iterator))
        channel_ids = [channel_id_2, channel_id_3, channel_id_4]
        response = self.client.post(self.url, data={"channel_ids": ",".join(channel_ids)})
        self.assertEqual("Added 3 manually tracked channels.", response.data)
        channels = self.manager.get(channel_ids)
        for channel in channels:
            self.assertEqual(channel.custom_properties.is_tracked, True)

    def test_channel_track_duplicate(self):
        user = self.create_test_user()
        Group.objects.get_or_create(name=PermissionGroupNames.AUDIT_VET)
        user.add_custom_user_permission("vet_audit")
        user.add_custom_user_group(PermissionGroupNames.AUDIT_VET)
        new_channel_id = str(next(int_iterator))
        new_channel_id_2 = str(next(int_iterator))
        new_channel_ids = [new_channel_id, new_channel_id_2]
        channel_ids = ",".join([self.channel_id_1] + new_channel_ids)
        response = self.client.post(self.url, data={"channel_ids": channel_ids})
        self.assertEqual(f"Added {len(new_channel_ids)} manually tracked channels.", response.data)
        old_channel = self.manager.get([self.channel_id_1])[0]
        new_channels = self.manager.get([new_channel_ids])
        self.assertEqual(old_channel.custom_properties.is_tracked, None)
        for new_channel in new_channels:
            self.assertEqual(new_channel.custom_properties.is_tracked, True)
