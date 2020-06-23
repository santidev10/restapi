import urllib
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
from utils.unittests.es_components_patcher import SearchDSLPatcher
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


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

    def test_brand_safety_filter(self):
        user = self.create_test_user()
        Group.objects.get_or_create(name=PermissionGroupNames.BRAND_SAFETY_SCORING)
        user.add_custom_user_permission("channel_list")
        user.add_custom_user_group(PermissionGroupNames.BRAND_SAFETY_SCORING)
        channel_id = str(next(int_iterator))
        channel_id_2 = str(next(int_iterator))
        channel_id_3 = str(next(int_iterator))
        channel_id_4 = str(next(int_iterator))
        channel_id_5 = str(next(int_iterator))

        channel = Channel(**{
            "meta": {
                "id": channel_id
            },
            "brand_safety": {
                "overall_score": 89
            }
        })
        channel_2 = Channel(**{
            "meta": {
                "id": channel_id_2
            },
            "brand_safety": {
                "overall_score": 98
            }
        })
        channel_3 = Channel(**{
            "meta": {
                "id": channel_id_3
            },
            "brand_safety": {
                "overall_score": 0
            }
        })
        channel_4 = Channel(**{
            "meta": {
                "id": channel_id_4
            },
            "brand_safety": {
                "overall_score": 75
            }
        })
        channel_5 = Channel(**{
            "meta": {
                "id": channel_id_5
            },
            "brand_safety": {
                "overall_score": 79
            }
        })
        sleep(1)
        sections = [Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.CMS, Sections.AUTH]
        ChannelManager(sections=sections).upsert([channel, channel_2, channel_3, channel_4, channel_5])
        high_risk_url = self.url + "?brand_safety=Unsuitable"
        risky_url = self.url + "?brand_safety=Low%20Suitability"
        low_risk_url = self.url + "?brand_safety=Medium%20Suitability"
        safe_url = self.url + "?brand_safety=Suitable"
        high_risk_and_safe_url = high_risk_url + "%2CSuitable"
        high_risk_response = self.client.get(high_risk_url)
        risky_response = self.client.get(risky_url)
        low_risk_response = self.client.get(low_risk_url)
        safe_response = self.client.get(safe_url)
        high_risk_and_safe_response = self.client.get(high_risk_and_safe_url)
        self.assertEqual(len(high_risk_response.data["items"]), 1)
        self.assertEqual(len(risky_response.data["items"]), 2)
        self.assertEqual(len(low_risk_response.data["items"]), 1)
        self.assertEqual(len(safe_response.data["items"]), 1)
        self.assertEqual(len(high_risk_and_safe_response.data["items"]), 2)
        self.assertEqual(
            89,
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

        self.assertEqual(len(response.data.get("items")), len(default_similar_channels))

    def test_ignore_monetization_filter_no_permission(self):
        user = self.create_test_user()
        user.add_custom_user_permission("channel_list")
        channels = [Channel(next(int_iterator)) for _ in range(2)]
        channels[0].populate_monetization(is_monetizable=True)

        ChannelManager([Sections.GENERAL_DATA, Sections.AUTH, Sections.MONETIZATION]).upsert(channels)

        response = self.client.get(self.url)
        self.assertEqual(len(response.data["items"]), len(channels))
        self.assertTrue(all(item.get("monetization") is None for item in response.data["items"]))

    def test_monetization_filter_has_permission(self):
        self.create_admin_user()
        channels = [Channel(next(int_iterator)) for _ in range(2)]
        channels[0].populate_monetization(is_monetizable=True)

        ChannelManager([Sections.GENERAL_DATA, Sections.AUTH, Sections.MONETIZATION]).upsert(channels)

        response = self.client.get(self.url + "?monetization.is_monetizable=true")
        data = response.data["items"]
        doc = data[0]
        self.assertTrue(len(data) == 1)
        self.assertTrue(doc["main"]["id"] == channels[0].main.id)
        self.assertTrue(doc["monetization"]["is_monetizable"] is True)

    def test_relevancy_score_sorting(self):
        """
        test that searching for results by relevancy (_score) asc/desc works
        note: multi_match phrase requires repetition of the phrase (which must also
        be in order) to get a diff in the score. Uses score of highest scoring field
        """
        user = self.create_test_user()
        user.add_custom_user_permission("channel_list")

        channel_ids = [str(next(int_iterator)) for i in range(2)]
        most_relevant_channel_title = "the quick brown fox the quick brown fox quick brown fox"
        most_relevant_channel = Channel(**{
            "meta": {
                "id": channel_ids[0],
            },
            "general_data": {
                "title": most_relevant_channel_title,
                "description": "the quick brown fox jumps over the lazy dog the quick brown fox jumps over the lazy "
                               "dog"
            }
        })
        least_relevant_channel = Channel(**{
            "meta": {
                "id": channel_ids[1],
            },
            "general_data": {
                "title": "the fox is quick and brown",
                "description": "woah did you see that? that quick brown fox jumped over a dog!",
            }
        })
        sections = [Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.CMS, Sections.AUTH]
        ChannelManager(sections=sections).upsert([most_relevant_channel, least_relevant_channel])

        search_term = "quick brown fox"
        # test sorting by _score:desc
        desc_url = self.url + "?" + urllib.parse.urlencode({
            "general_data.title": search_term,
            "general_data.description": search_term,
            "sort": "_score:desc",
        })
        desc_response = self.client.get(desc_url)
        desc_items = desc_response.data["items"]
        self.assertEqual(desc_items[0]["general_data"]["title"], most_relevant_channel_title)

        # test sort _score:asc
        asc_url = self.url + "?" + urllib.parse.urlencode({
            "general_data.title": search_term,
            "general_data.description": search_term,
            "sort": "_score:asc",
        })
        asc_response = self.client.get(asc_url)
        asc_items = asc_response.data["items"]
        self.assertEqual(asc_items[-1]["general_data"]["title"], most_relevant_channel_title)
