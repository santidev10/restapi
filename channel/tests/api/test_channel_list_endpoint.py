from time import sleep
from unittest.mock import patch

from django.contrib.auth.models import Group
from django.test import override_settings
from elasticsearch_dsl import Double
from rest_framework.status import HTTP_200_OK

from channel.api.urls.names import ChannelPathName
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.models.base import BaseDocument
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from utils.brand_safety_view_decorator import get_brand_safety_label
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
        channel = Channel(channel_id)
        ChannelManager(sections=[Sections.GENERAL_DATA]).upsert([channel])
        score = 92
        label = get_brand_safety_label(score)
        brand_safety = ChannelBrandSafetyDoc(
            meta={'id': channel_id},
            overall_score=score
        )
        brand_safety.save()
        sleep(1)

        with override_settings(BRAND_SAFETY_CHANNEL_INDEX=ChannelBrandSafetyDoc._index._name):
            response = self.client.get(self.url)

        self.assertEqual(
            {"score": brand_safety.overall_score, "label": label},
            response.data["items"][0]["brand_safety_data"]
        )


class ChannelBrandSafetyDoc(BaseDocument):
    """
    Temporary solution for testing brand safety.
    Remove this doc after implementing the Brand Safety feature in the dmp project
    """
    overall_score = Double()

    class Index:
        name = "channel_brand_safety"
        prefix = "channel_brand_safety_"

    class Meta:
        doc_type = "channel_brand_safety"
