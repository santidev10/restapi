from time import sleep
from unittest.mock import patch

from django.contrib.auth.models import Group
from rest_framework.status import HTTP_200_OK

from channel.api.urls.names import ChannelPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from utils.utittests.es_components_patcher import SearchDSLPatcher
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class ChannelListTestCase(ExtendedAPITestCase, ESTestCase):
    url = reverse(ChannelPathName.CHANNEL_LIST, [Namespace.CHANNEL])

    def test_simple_list_works(self):
        with patch("es_components.managers.channel.ChannelManager.search",
                   return_value=SearchDSLPatcher()):
            self.create_admin_user()
            response = self.client.get(self.url)
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_brand_safety(self):
        user = self.create_test_user()
        Group.objects.get_or_create(name=PermissionGroupNames.BRAND_SAFETY_SCORING)
        user.add_custom_user_permission("channel_list")
        user.add_custom_user_group(PermissionGroupNames.BRAND_SAFETY_SCORING)
        channel_id = str(next(int_iterator))
        score = 92
        channel = Channel(**{
            "meta": {
                "id": channel_id
            },
            "brand_safety": {
                "overall_score": score
            }
        })
        sleep(1)
        ChannelManager(sections=[Sections.GENERAL_DATA, Sections.BRAND_SAFETY]).upsert([channel])
        response = self.client.get(self.url)
        self.assertEqual(
            score,
            response.data["items"][0]["brand_safety"]["overall_score"]
        )

    def test_extra_fields(self):
        self.create_admin_user()
        extra_fields = ("brand_safety_data", "chart_data", "blacklist_data")
        channel = Channel(str(next(int_iterator)))
        ChannelManager([Sections.GENERAL_DATA]).upsert([channel])

        response = self.client.get(self.url)

        for field in extra_fields:
            with self.subTest(field):
                self.assertIn(field, response.data["items"][0])
