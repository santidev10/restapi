import datetime
from datetime import timedelta
from datetime import datetime
import pickle
from time import sleep
import urllib
from urllib.parse import urlencode
from unittest.mock import patch

from django.test import override_settings
from django.utils import timezone
from elasticsearch_dsl import Q
from rest_framework.status import HTTP_200_OK

from audit_tool.models import IASHistory
from brand_safety import constants
from channel.api.urls.names import ChannelPathName
from channel.models import AuthChannel
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.aggregation_constants import ALLOWED_CHANNEL_AGGREGATIONS
from utils.es_components_cache import flush_cache
from utils.redis import get_redis_client
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

    def test_own_channels(self):
        """ Test that authenticated OAuthed users should be get own youtube channels """
        self.create_test_user()
        with patch("es_components.managers.channel.ChannelManager.search",
                   return_value=SearchDSLPatcher()):
            url = self.url + "?own_channels=1"
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_brand_safety(self):
        self.create_test_user(perms={
            StaticPermissions.RESEARCH__BRAND_SUITABILITY: True,
        })
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
        self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
            StaticPermissions.RESEARCH__BRAND_SUITABILITY: True,
        })
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
        risky_url = self.url + "?brand_safety=Low%20Suitability"
        low_risk_url = self.url + "?brand_safety=Medium%20Suitability"
        safe_url = self.url + "?brand_safety=Suitable"
        low_risk_and_safe_url = low_risk_url + "%2CSuitable"
        risky_response = self.client.get(risky_url)
        low_risk_response = self.client.get(low_risk_url)
        safe_response = self.client.get(safe_url)
        low_risk_and_safe = self.client.get(low_risk_and_safe_url)
        self.assertEqual(len(risky_response.data["items"]), 2)
        self.assertEqual(len(low_risk_response.data["items"]), 1)
        self.assertEqual(len(safe_response.data["items"]), 1)
        self.assertEqual(len(low_risk_and_safe.data["items"]), 2)
        self.assertEqual(
            89,
            low_risk_response.data["items"][0]["brand_safety"]["overall_score"]
        )

    def test_brand_safety_high_risk_permission(self):
        """
        test that a regular user can filter on RISKY or above scores, while
        RESEARCH__BRAND_SUITABILITY_HIGH_RISK users can additionally filter on HIGH_RISK scores
        """
        user = self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
            StaticPermissions.RESEARCH__BRAND_SUITABILITY: True,
        })

        channel_id = str(next(int_iterator))
        channel_id_2 = str(next(int_iterator))
        channel = Channel(**{
            "meta": {
                "id": channel_id
            },
            "brand_safety": {
                "overall_score": 77
            }
        })
        channel_2 = Channel(**{
            "meta": {
                "id": channel_id_2
            },
            "brand_safety": {
                "overall_score": 61
            }
        })
        sections = [Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.CMS, Sections.AUTH]
        ChannelManager(sections=sections).upsert([channel, channel_2])
        url = self.url + "?" + urlencode({
            "brand_safety": ",".join([constants.RISKY, constants.HIGH_RISK])
        })

        # regular user, no high risk allowed
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 1)

        # regular admin, all filters available
        user.perms.update({
            StaticPermissions.ADMIN: True,
        })
        user.save()
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 2)

        # RESEARCH__BRAND_SUITABILITY_HIGH_RISK perm should see HIGH_RISK agg
        user.perms.update({
            StaticPermissions.ADMIN: False,
            StaticPermissions.RESEARCH__BRAND_SUITABILITY_HIGH_RISK: True,
        })
        user.save()
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 2)

    def test_extra_fields(self):
        self.create_admin_user()
        extra_fields = ("brand_safety_data", "chart_data", "blacklist_data", "task_us_data",)
        extra_fields_map = {
            "task_us_data": "vetted_status"
        }
        channel = Channel(str(next(int_iterator)))
        ChannelManager([Sections.GENERAL_DATA, Sections.CMS, Sections.AUTH]).upsert([channel])

        response = self.client.get(self.url)

        first_item = response.data["items"][0]
        for field in extra_fields:
            with self.subTest(field):
                if field in extra_fields_map.keys():
                    self.assertIn(extra_fields_map.get(field), first_item)
                else:
                    self.assertIn(field, first_item)

    def test_filter_by_ids(self):
        self.create_admin_user()
        items_to_filter = 2
        channels = [Channel(next(int_iterator)) for _ in range(items_to_filter + 1)]
        ChannelManager([Sections.GENERAL_DATA, Sections.AUTH, Sections.CMS]).upsert(channels)

        url = self.url + "?" + urllib.parse.urlencode({
            "main.id": ",".join([str(video.main.id) for video in channels[:items_to_filter]])
        })
        response = self.client.get(url)

        self.assertEqual(items_to_filter, len(response.data["items"]))

    def test_filter_by_single_id(self):
        self.create_admin_user()
        items_to_filter = 1
        channels = [Channel(next(int_iterator)) for _ in range(items_to_filter + 1)]
        ChannelManager([Sections.GENERAL_DATA, Sections.AUTH, Sections.CMS]).upsert(channels)

        url = self.url + "?" + urllib.parse.urlencode({
            "main.id": channels[0].main.id
        })
        response = self.client.get(url)

        self.assertEqual(items_to_filter, len(response.data["items"]))

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
        self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
        })
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
        self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
        })

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

    def test_content_quality_filter(self):
        self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
        })
        manager = ChannelManager(sections=(Sections.TASK_US_DATA,))
        docs = [
            manager.model(
                main={"id": f"test_channel_{next(int_iterator)}"},
                task_us_data={"content_quality": 1}
            ),
            manager.model(
                main={"id": f"test_channel_{next(int_iterator)}"}
            )
        ]
        manager.upsert(docs)
        url = self.url + "?" + urllib.parse.urlencode({
            "task_us_data.content_quality": "Average",
        })
        with patch.object(ChannelManager, "forced_filters", return_value=Q()):
            data = self.client.get(url).data
        self.assertEqual(data["items_count"], 1)
        self.assertEqual(data["items"][0]["main"]["id"], docs[0].main.id)

    def test_channel_id_query_param_mutation(self):
        """
        Test that a search on a channel id correctly mutates the
        query params to return that channel only, even where
        the search term exists in a field that is specified in
        the initial search
        """
        user = self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
        })

        channel_ids = [str(next(int_iterator)) for i in range(3)]
        channel_one = Channel(**{
            "meta": {"id": channel_ids[0]},
            "main": {'id': channel_ids[0]},
            "general_data": {
                "title": "channel with id we're searching for",
                "description": f"some description."
            }
        })
        channel_two = Channel(**{
            "meta": {"id": channel_ids[1]},
            "main": {'id': channel_ids[1]},
            "general_data": {
                "title": "the fox is quick",
                "description": f"some description. {channel_ids[0]}"
            }
        })
        channel_three = Channel(**{
            "meta": {"id": channel_ids[2]},
            "main": {'id': channel_ids[2]},
            "general_data": {
                "title": "the fox is quick and brown",
                "description": f"some description. {channel_ids[0]}"
            }
        })
        sections = [Sections.GENERAL_DATA, Sections.MAIN, Sections.BRAND_SAFETY, Sections.CMS, Sections.AUTH]
        ChannelManager(sections=sections).upsert([channel_one, channel_two, channel_three])

        search_term = channel_ids[0]
        url = self.url + "?" + urllib.parse.urlencode({
            "general_data.title": search_term,
            "general_data.description": search_term,
        })
        response = self.client.get(url)
        items = response.data['items']
        self.assertEqual(items[0]['main']['id'], channel_ids[0])

    def test_vetted_status_field(self):
        self.create_admin_user()
        channel_ids = []
        for i in range(6):
            channel_ids.append(str(next(int_iterator)))

        channels = []
        # vetted
        channels.append(Channel(**{
            "meta": {"id": channel_ids[0]},
            "main": {'id': channel_ids[0]},
            "general_data": {
                "title": f"channel: {channel_ids[0]}",
                "description": f"this channel is vetted safe. Channel id: {channel_ids[0]}"
            },
            "task_us_data": {
                "last_vetted_at": timezone.now(),
                "brand_safety": [None,],
            },
        }))
        channels.append(Channel(**{
            "meta": {"id": channel_ids[1]},
            "main": {'id': channel_ids[1]},
            "general_data": {
                "title": f"channel: {channel_ids[1]}",
                "description": f"this channel is vetted safe. Channel id: {channel_ids[1]}"
            },
            "task_us_data": {
                "last_vetted_at": timezone.now(),
            },
        }))
        channels.append(Channel(**{
            "meta": {"id": channel_ids[2]},
            "main": {'id': channel_ids[2]},
            "general_data": {
                "title": f"channel: {channel_ids[2]}",
                "description": f"this channel is vetted risky. Channel id: {channel_ids[2]}"
            },
            "task_us_data": {
                "last_vetted_at": timezone.now(),
                "brand_safety": [1, 2, 3, 4],
            },
        }))
        # unvetted
        channels.append(Channel(**{
            "meta": {"id": channel_ids[3]},
            "main": {'id': channel_ids[3]},
            "general_data": {
                "title": f"channel: {channel_ids[3]}",
                "description": f"this channel is not vetted. Channel id: {channel_ids[3]}"
            },
            "task_us_data": {
                "brand_safety": [1,],
            },
        }))
        channels.append(Channel(**{
            "meta": {"id": channel_ids[4]},
            "main": {'id': channel_ids[4]},
            "general_data": {
                "title": f"channel: {channel_ids[4]}",
                "description": f"this channel is not vetted. Channel id: {channel_ids[4]}"
            },
            "task_us_data": {},
        }))
        channels.append(Channel(**{
            "meta": {"id": channel_ids[5]},
            "main": {'id': channel_ids[5]},
            "general_data": {
                "title": f"channel: {channel_ids[5]}",
                "description": f"this channel is not vetted. Channel id: {channel_ids[5]}"
            },
        }))

        ChannelManager([Sections.GENERAL_DATA, Sections.CMS, Sections.AUTH, Sections.TASK_US_DATA]).upsert(channels)

        response = self.client.get(self.url)
        items = response.data['items']

        vetted_statuses = []
        for item in items:
            status = item.get('vetted_status', None)
            vetted_statuses.append(status)
        unvetted = [status for status in vetted_statuses if status == "Unvetted"]
        safe = [status for status in vetted_statuses if status == "Vetted Safe"]
        risky = [status for status in vetted_statuses if status == "Vetted Risky"]
        self.assertEqual(len(unvetted), 3)
        self.assertEqual(len(safe), 2)
        self.assertEqual(len(risky), 1)

        unvetted_response = self.client.get(self.url + "?task_us_data.last_vetted_at=false")
        unvetted_items = unvetted_response.data['items']
        unvetted_channel_ids = channel_ids[3:]
        self.assertEqual([item['main']['id'] for item in unvetted_items].sort(), unvetted_channel_ids.sort())

        vetted_response = self.client.get(self.url + "?task_us_data.last_vetted_at=true")
        vetted_items = vetted_response.data['items']
        vetted_channel_ids = channel_ids[:3]
        self.assertEqual([item['main']['id'] for item in vetted_items].sort(), vetted_channel_ids.sort())

    def test_permissions(self):
        user = self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
        })
        channel_id = str(next(int_iterator))
        channel = Channel(**{
            "meta": {"id": channel_id},
            "main": {'id': channel_id},
            "general_data": {
                "title": f"channel: {channel_id}",
                "description": f"this channel is vetted safe. Channel id: {channel_id}"
            },
            "task_us_data": {
                "last_vetted_at": timezone.now(),
                "brand_safety": [None,],
            },
        })

        ChannelManager([Sections.GENERAL_DATA, Sections.CMS, Sections.AUTH, Sections.TASK_US_DATA]).upsert([channel])

        # normal user
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data['items']
        self.assertEqual(len(items), 1)
        item = items[0]
        item_fields = list(item.keys())
        self.assertNotIn("vetted_status", item_fields)
        self.assertNotIn("blacklist_data", item_fields)

        # audit vet admin
        user.perms.update({
            StaticPermissions.RESEARCH__VETTING_DATA: True,
        })
        user.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data['items']
        self.assertEqual(len(items), 1)
        item = items[0]
        item_fields = list(item.keys())
        self.assertIn("vetted_status", item_fields)
        self.assertNotIn("blacklist_data", item_fields)

        # blocklist
        user.perms.update({
            StaticPermissions.RESEARCH__VETTING_DATA: False,
            StaticPermissions.RESEARCH__BRAND_SUITABILITY_HIGH_RISK: True,
        })
        user.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data['items']
        self.assertEqual(len(items), 1)
        item = items[0]
        item_fields = list(item.keys())
        self.assertNotIn("vetted_status", item_fields)
        self.assertIn("blacklist_data", item_fields)

    def test_vetting_data_perm_aggregations_guard(self):
        user = self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
            StaticPermissions.RESEARCH__VETTING_DATA: False,
        })

        channel_ids = []
        for i in range(2):
            channel_ids.append(str(next(int_iterator)))
        channels = []
        # vetted
        channels.append(Channel(**{
            "meta": {"id": channel_ids[0]},
            "main": {'id': channel_ids[0]},
            "general_data": {
                "title": f"channel: {channel_ids[0]}",
                "description": f"this channel is vetted safe. Channel id: {channel_ids[0]}"
            },
            "task_us_data": {
                "last_vetted_at": timezone.now(),
                "brand_safety": [None,],
            },
        }))
        channels.append(Channel(**{
            "meta": {"id": channel_ids[1]},
            "main": {'id': channel_ids[1]},
            "general_data": {
                "title": f"channel: {channel_ids[1]}",
                "description": f"this channel is not vetted. Channel id: {channel_ids[1]}"
            },
        }))

        ChannelManager([Sections.GENERAL_DATA, Sections.CMS, Sections.AUTH, Sections.TASK_US_DATA]).upsert(channels)

        url = self.url + "?" + urlencode({
            "aggregations": ",".join(ALLOWED_CHANNEL_AGGREGATIONS),
        })
        vetting_admin_aggregations = ['task_us_data.last_vetted_at:exists', 'task_us_data.last_vetted_at:missing']

        # normal user should not see vetting admin aggs
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        response_aggregations = response.data['aggregations']
        response_aggregation_keys = list(response_aggregations.keys())
        for aggregation in vetting_admin_aggregations:
            with self.subTest(aggregation):
                self.assertNotIn(aggregation, response_aggregation_keys)

        # admin should see aggs
        user.perms.update({
            StaticPermissions.ADMIN: True,
        })
        user.save()
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        response_aggregations = response.data['aggregations']
        response_aggregation_keys = list(response_aggregations.keys())
        for aggregation in vetting_admin_aggregations:
            with self.subTest(aggregation):
                self.assertIn(aggregation, response_aggregation_keys)

        # vetting admin should see aggs
        user.perms.update({
            StaticPermissions.ADMIN: False,
            StaticPermissions.RESEARCH__VETTING_DATA: True,
        })
        user.save()
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        response_aggregations = response.data['aggregations']
        response_aggregation_keys = list(response_aggregations.keys())
        for aggregation in vetting_admin_aggregations:
            with self.subTest(aggregation):
                self.assertIn(aggregation, response_aggregation_keys)

    def test_non_admin_brand_safety_exclusion(self):
        user = self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
        })

        url = self.url + "?" + urlencode({
            "aggregations": ",".join(ALLOWED_CHANNEL_AGGREGATIONS),
        })

        # normal user should not see HIGH_RISK brand safety agg
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        response_aggregations = response.data["aggregations"]
        self.assertIn(constants.BRAND_SAFETY, list(response_aggregations.keys()))
        buckets = response_aggregations[constants.BRAND_SAFETY]["buckets"]
        self.assertEqual(len(buckets), 3)
        labels = [bucket['key'] for bucket in buckets]
        self.assertNotIn(constants.HIGH_RISK, labels)

        # admin should see HIGH_RISK agg
        user.perms.update({
            StaticPermissions.ADMIN: True,
        })
        user.save()
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        response_aggregations = response.data["aggregations"]
        self.assertIn(constants.BRAND_SAFETY, list(response_aggregations.keys()))
        buckets = response_aggregations[constants.BRAND_SAFETY]["buckets"]
        self.assertEqual(len(buckets), 4)
        labels = [bucket['key'] for bucket in buckets]
        self.assertIn(constants.HIGH_RISK, labels)

        # RESEARCH__BRAND_SUITABILITY_HIGH_RISK should see HIGH_RISK agg
        user.perms.update({
            StaticPermissions.ADMIN: False,
            StaticPermissions.RESEARCH__BRAND_SUITABILITY_HIGH_RISK: True,
        })
        user.save()
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        response_aggregations = response.data["aggregations"]
        self.assertIn(constants.BRAND_SAFETY, list(response_aggregations.keys()))
        buckets = response_aggregations[constants.BRAND_SAFETY]["buckets"]
        self.assertEqual(len(buckets), 4)
        labels = [bucket['key'] for bucket in buckets]
        self.assertIn(constants.HIGH_RISK, labels)

    def test_channel_ias_data(self):
        """ Test that a Channel is serialized with IAS data only if it was included in the latest IAS ingestion """
        self.create_admin_user()
        now = timezone.now()
        channel_manager = ChannelManager((Sections.IAS_DATA, Sections.GENERAL_DATA, Sections.STATS))
        latest_ias = IASHistory.objects.create(name="", started=now, completed=now)
        channel_outdated_ias = Channel(f"channel_{next(int_iterator)}")
        channel_outdated_ias.populate_general_data(title="test")
        channel_outdated_ias.populate_ias_data(ias_verified=now - timedelta(days=1))
        channel_outdated_ias.populate_stats(total_videos_count=1)

        channel_current_ias = Channel(f"channel_{next(int_iterator)}")
        channel_current_ias.populate_general_data(title="test")
        channel_current_ias.populate_ias_data(ias_verified=latest_ias.started)
        channel_current_ias.populate_stats(total_videos_count=1)

        channel_manager.upsert([channel_outdated_ias, channel_current_ias])
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = sorted(response.data["items"], key=lambda x: x["main"]["id"])

        self.assertEqual(data[0]["main"]["id"], channel_outdated_ias.main.id)
        self.assertIsNone(data[0].get("ias_data"))

        self.assertEqual(data[1]["main"]["id"], channel_current_ias.main.id)
        self.assertIsNotNone(data[1].get("ias_data"))

    def test_cache(self):
        """ Test subsequent requests uses cache """
        self.create_admin_user()
        flush_cache()
        url = self.url + "?page=1&fields=main&sort=stats.views:desc"
        self.client.get(url)
        with patch("utils.es_components_cache.set_to_cache") as mock_set_cache,\
                override_settings(ES_CACHE_ENABLED=True):
            # Subsequent requests should use cache to retrieve but not set
            self.client.get(url)
        mock_set_cache.assert_not_called()
        flush_cache()

    def test_relevancy_score_sorting_with_category_filter(self):
        """
        test that searching for results by relevancy (_score) asc/desc works
        when category filter is selected. Result items with matching
        primary categories should appear first with a +10 scoring boost when
        relevancy sorting is descending.
        """
        self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
        })

        channel_ids = [str(next(int_iterator)) for i in range(2)]
        primary_category = "Music & Audio"
        most_relevant_channel = Channel(**{
            "meta": {
                "id": channel_ids[0],
            },
            "general_data": {
                "iab_categories": ["Music & Audio", "Social"],
                "primary_category": primary_category
            }
        })
        least_relevant_channel = Channel(**{
            "meta": {
                "id": channel_ids[1],
            },
            "general_data": {
                "iab_categories": ["Music & Audio", "Social"],
                "primary_category": "Social"
            }
        })
        sections = [Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.CMS, Sections.AUTH]
        ChannelManager(sections=sections).upsert([most_relevant_channel, least_relevant_channel])

        # test sorting by _score:desc
        desc_url = self.url + "?" + urllib.parse.urlencode({
            "general_data.iab_categories": "Music & Audio",
            "sort": "_score:desc",
        })
        desc_response = self.client.get(desc_url)
        desc_items = desc_response.data["items"]
        self.assertEqual(desc_items[0]["general_data"]["primary_category"], primary_category)

        # test sort _score:asc
        asc_url = self.url + "?" + urllib.parse.urlencode({
            "general_data.iab_categories": "Music & Audio",
            "sort": "_score:asc",
        })
        asc_response = self.client.get(asc_url)
        asc_items = asc_response.data["items"]
        self.assertEqual(asc_items[-1]["general_data"]["primary_category"], primary_category)

    def test_research_phrase_starts_with(self):
        """
        test that searching for channel will yield results of channels that has a title or description
        with phrase that starts with what the user provided as input.
        """
        self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
            StaticPermissions.RESEARCH__CHANNEL_VIDEO_DATA: True,
        })

        channel_id = str(next(int_iterator))
        channel_id_2 = str(next(int_iterator))
        channel_id_3 = str(next(int_iterator))
        channel_id_4 = str(next(int_iterator))
        channel_id_5 = str(next(int_iterator))

        most_relevant_channel = Channel(**{
            "meta": {
                "id": channel_id,
            },
            "general_data": {
                "title": "watchmojo",
                "description": "the quick brown fox jumps over the lazy dog",
            }
        })
        relevant_channel2 = Channel(**{
            "meta": {
                "id": channel_id_2,
            },
            "general_data": {
                "title": "watchmojo.com",
                "description": "woah did you see that? that quick brown fox jumped over a dog!",
            }
        })
        relevant_channel3 = Channel(**{
            "meta": {
                "id": channel_id_3,
            },
            "general_data": {
                "title": "another relevant channel",
                "description": "woah did you see that? watchmojo brown fox jumped over a dog!",
            }
        })
        relevant_channel4 = Channel(**{
            "meta": {
                "id": channel_id_4,
            },
            "general_data": {
                "title": "fourth channel",
                "description": "woah did you see that? watchmojo.com brown fox jumped over a dog!",
            }
        })
        not_relevant_channel5 = Channel(**{
            "meta": {
                "id": channel_id_5,
            },
            "general_data": {
                "title": "not related",
                "description": "woah did you see that? brown fox jumped over a dog!",
            }
        })
        sleep(1)
        sections = [Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.CMS, Sections.AUTH]
        ChannelManager(sections=sections).upsert([most_relevant_channel, relevant_channel2,
                                                  relevant_channel3, relevant_channel4, not_relevant_channel5])

        # test sorting by _score:desc
        desc_url = self.url + "?" + urllib.parse.urlencode({
            "general_data.title": "watchmojo",
            "sort": "_score:desc",
        })
        desc_response = self.client.get(desc_url)
        desc_items = desc_response.data["items"]
        self.assertEqual(len(desc_items), 4)
        self.assertEqual(desc_items[0]["general_data"]["title"], "watchmojo")
        self.assertEqual(desc_items[1]["general_data"]["title"], "watchmojo.com")


        # test sort _score:asc
        asc_url = self.url + "?" + urllib.parse.urlencode({
            "general_data.title": "watchmojo",
            "sort": "_score:asc",
        })
        asc_response = self.client.get(asc_url)
        asc_items = asc_response.data["items"]
        self.assertEqual(len(asc_items), 4)
        self.assertEqual(asc_items[-1]["general_data"]["title"], "watchmojo")
        self.assertEqual(asc_items[-2]["general_data"]["title"], "watchmojo.com")

    def test_mapped_fields(self):
        """
        ensure that fields whose values are mapped from other values exist, and are correct
        :return:
        """
        self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
        })

        channel_ids = [str(next(int_iterator)) for i in range(2)]
        channel = Channel(**{
            "meta": {
                "id": channel_ids[0],
            },
            "general_data": {
                "country_code": "US",
                "top_lang_code": "en",
            }
        })
        sections = [Sections.GENERAL_DATA, Sections.BRAND_SAFETY, Sections.CMS, Sections.AUTH]
        ChannelManager(sections=sections).upsert([channel,])

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data.get("items")
        self.assertTrue(len(items), 1)
        res_channel = items[0]
        self.assertEqual(res_channel.get(Sections.GENERAL_DATA, {}).get("country"), "United States")
        self.assertTrue(res_channel.get(Sections.GENERAL_DATA, {}).get("top_language"), "English")

    def test_ias_verified_filter(self):
        """
        test that ias verified filters based on the last completed IASHistory's `started` timestamp
        :return:
        """
        self.create_test_user(perms={
            StaticPermissions.RESEARCH: True,
        })

        now = timezone.now()
        unverified = []
        for _ in range(5):
            id = next(int_iterator)
            unverified.append(Channel(**{
                "meta": {
                    "id": f"channel_{id}",
                },
                "general_data": {
                    "title": f"title_{id}",
                    "description": "description"
                },
                "ias_data": {
                    "ias_verified": now - timedelta(minutes=1)
                }
            }))

        IASHistory.objects.create(name="asdf", started=now, completed=now + timedelta(minutes=1))

        verified = []
        for _ in range(8):
            id = next(int_iterator)
            unverified.append(Channel(**{
                "meta": {
                    "id": f"channel_{id}",
                },
                "general_data": {
                    "title": f"title_{id}",
                    "description": "description"
                },
                "ias_data": {
                    "ias_verified": now
                }
            }))

        filtered_url = self.url + "?" + urllib.parse.urlencode({
            "ias_data.ias_verified": "true",
        })
        response = self.client.get(filtered_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(verified), response.data.get("items_count"))

    def test_auth_channel_filter(self):
        """ Tests that auth channel filter only pulls channels with matching AuthChannel object and active token """
        self.create_admin_user()

        manager = ChannelManager(upsert_sections=[Sections.MAIN, Sections.GENERAL_DATA,])
        channels = manager.get_or_create(ids=[f"channel_{next(int_iterator)}" for _ in range(5)])
        manager.upsert(channels)
        ids = [item.main.id for item in channels]

        AuthChannel.objects.create(channel_id=ids[0], token_revocation=None)
        AuthChannel.objects.create(channel_id=ids[2], token_revocation=datetime.now())
        AuthChannel.objects.create(channel_id=ids[4], token_revocation=None)

        filtered_url = self.url + "?" + urllib.parse.urlencode({
            "auth_channel": "true",
        })
        with patch.object(ChannelManager, "forced_filters", return_value=Q()):
            response = self.client.get(filtered_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items_count"), 2)
        self.assertEqual(response.data['items'][0]['main']['id'], ids[0])
        self.assertEqual(response.data['items'][1]['main']['id'], ids[4])

    def test_get_default_cache(self):
        """ Test channel caching with default sort of stats.subscribers:desc """
        self.create_admin_user()
        url = self.url + "?sort=stats.subscribers:desc"
        with self.subTest("Cache is not used if not first page"),\
                patch.object(pickle, "loads") as mock_loads:
            response = self.client.get(url + "&page=2")
            self.assertEqual(response.status_code, HTTP_200_OK)
            mock_loads.assert_not_called()

        with self.subTest("Cache is not used if filters applied"),\
                patch.object(pickle, "loads") as mock_loads:
            response = self.client.get(url + "&brand_safety=Suitable")
            self.assertEqual(response.status_code, HTTP_200_OK)
            mock_loads.assert_not_called()

        with self.subTest("Cache used if first page with no filters"), \
                patch.object(pickle, "loads", return_value=[]) as mock_loads:
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            mock_loads.assert_called_once()
