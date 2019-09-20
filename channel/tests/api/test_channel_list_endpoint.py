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
        sections = [Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.CMS, Sections.AUTH]
        ChannelManager(sections=sections).upsert([channel])
        response = self.client.get(self.url)
        self.assertEqual(
            score,
            response.data["items"][0]["brand_safety"]["overall_score"]
        )

    # def test_brand_safety_filter(self):
    #     user = self.create_test_user()
    #     Group.objects.get_or_create(name=PermissionGroupNames.BRAND_SAFETY_SCORING)
    #     user.add_custom_user_permission("channel_list")
    #     user.add_custom_user_permission("userprofile.scoring_brand_safety")
    #     user.add_custom_user_group(PermissionGroupNames.BRAND_SAFETY_SCORING)
    #     channel_id = str(next(int_iterator))
    #     channel_id_2 = str(next(int_iterator))
    #     channel_id_3 = str(next(int_iterator))
    #     channel_id_4 = str(next(int_iterator))
    #     channel_id_5 = str(next(int_iterator))
    #
    #     channel = Channel(**{
    #         "meta": {
    #             "id": channel_id
    #         },
    #         "brand_safety": {
    #             "overall_score": 89
    #         }
    #     })
    #     channel_2 = Channel(**{
    #         "meta": {
    #             "id": channel_id_2
    #         },
    #         "brand_safety": {
    #             "overall_score": 98
    #         }
    #     })
    #     channel_3 = Channel(**{
    #         "meta": {
    #             "id": channel_id_3
    #         },
    #         "brand_safety": {
    #             "overall_score": 0
    #         }
    #     })
    #     channel_4 = Channel(**{
    #         "meta": {
    #             "id": channel_id_4
    #         },
    #         "brand_safety": {
    #             "overall_score": 75
    #         }
    #     })
    #     channel_5 = Channel(**{
    #         "meta": {
    #             "id": channel_id_5
    #         },
    #         "brand_safety": {
    #             "overall_score": 79
    #         }
    #     })
    #     sleep(1)
    #     sections = [Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.CMS, Sections.AUTH]
    #     ChannelManager(sections=sections).upsert([channel, channel_2, channel_3, channel_4, channel_5])
    #     high_risk_url = self.url + "?brand_safety=High%20Risk"
    #     risky_url = self.url + "?brand_safety=Risky"
    #     low_risk_url = self.url + "?brand_safety=Low%20Risk"
    #     safe_url = self.url + "?brand_safety=Safe"
    #     high_risk_and_safe_url = high_risk_url + "%2CSafe"
    #     high_risk_response = self.client.get(high_risk_url)
    #     risky_response = self.client.get(risky_url)
    #     low_risk_response = self.client.get(low_risk_url)
    #     safe_response = self.client.get(safe_url)
    #     high_risk_and_safe_response = self.client.get(high_risk_and_safe_url)
    #     self.assertEqual(len(high_risk_response.data["items"]), 1)
    #     self.assertEqual(len(risky_response.data["items"]), 2)
    #     self.assertEqual(len(low_risk_response.data["items"]), 1)
    #     self.assertEqual(len(safe_response.data["items"]), 1)
    #     self.assertEqual(len(high_risk_and_safe_response.data["items"]), 2)
    #     self.assertEqual(
    #         89,
    #         low_risk_response.data["items"][0]["brand_safety"]["overall_score"]
        )

    def test_brand_safety_filter(self):
        user = self.create_test_user()
        Group.objects.get_or_create(name=PermissionGroupNames.BRAND_SAFETY_SCORING)
        user.add_custom_user_permission("channel_list")
        user.add_custom_user_group(PermissionGroupNames.BRAND_SAFETY_SCORING)
        channel_id = str(next(int_iterator))
        score = 89
        channel = Channel(**{
            "meta": {
                "id": channel_id
            },
            "brand_safety": {
                "overall_score": score
            }
        })
        sleep(1)
        sections = [Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.CMS, Sections.AUTH]
        ChannelManager(sections=sections).upsert([channel])
        high_risk_url = self.url + "?brand_safety.overall_score=%2C69"
        risky_url = self.url + "?brand_safety.overall_score=70%2C79"
        low_risk_url = self.url + "?brand_safety.overall_score=80%2C89"
        safe_url = self.url + "?brand_safety.overall_score=90%2C100"
        high_risk_response = self.client.get(high_risk_url)
        risky_response = self.client.get(risky_url)
        low_risk_response = self.client.get(low_risk_url)
        safe_response = self.client.get(safe_url)
        self.assertEqual(len(high_risk_response.data["items"]), 0)
        self.assertEqual(len(risky_response.data["items"]), 0)
        self.assertEqual(len(low_risk_response.data["items"]), 1)
        self.assertEqual(len(safe_response.data["items"]), 0)
        self.assertEqual(
            score,
            low_risk_response.data["items"][0]["brand_safety"]["overall_score"]
        )


    def test_extra_fields(self):
        self.create_admin_user()
        extra_fields = ("brand_safety_data", "chart_data", "blacklist_data")
        channel = Channel(str(next(int_iterator)))
        ChannelManager([Sections.GENERAL_DATA, Sections.CMS, Sections.AUTH]).upsert([channel])

        response = self.client.get(self.url)

        for field in extra_fields:
            with self.subTest(field):
                self.assertIn(field, response.data["items"][0])

    def test_similar_channels(self):
        self.create_admin_user()

        channel = Channel("test_channel")
        default_similar_channels = [Channel(str(i)) for i in range(20)]

        ChannelManager([Sections.GENERAL_DATA, Sections.AUTH]).upsert(default_similar_channels)

        channel.populate_similar_channels(
            default=[channel.main.id for channel in default_similar_channels]
        )
        ChannelManager([Sections.GENERAL_DATA, Sections.AUTH, Sections.SIMILAR_CHANNELS]).upsert([channel])

        response = self.client.get(self.url + "?similar_to=test_channel")

        self.assertEqual(len(response.data.get('items')), len(default_similar_channels))

