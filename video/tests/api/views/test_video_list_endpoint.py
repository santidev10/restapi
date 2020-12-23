import urllib
from urllib.parse import urlencode
from unittest.mock import PropertyMock
from unittest.mock import patch

from django.utils import timezone
from django.test import override_settings
from django.contrib.auth.models import Group
from rest_framework.status import HTTP_200_OK

from brand_safety import constants
from es_components.constants import Sections
from es_components.managers import VideoManager
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.permissions import PermissionGroupNames
from utils.aggregation_constants import ALLOWED_VIDEO_AGGREGATIONS
from utils.api.research import ResearchPaginator
from utils.es_components_cache import get_redis_client
from utils.es_components_cache import flush_cache
from utils.unittests.es_components_patcher import SearchDSLPatcher
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.segment_functionality_mixin import SegmentFunctionalityMixin
from utils.unittests.test_case import ExtendedAPITestCase
from video.api.urls.names import Name


class VideoListTestCase(ExtendedAPITestCase, SegmentFunctionalityMixin, ESTestCase):
    def get_url(self, **kwargs):
        return reverse(Name.VIDEO_LIST, [Namespace.VIDEO], query_params=kwargs)

    def test_simple_list_works(self):
        self.create_admin_user()
        with patch("es_components.managers.video.VideoManager.search",
                   return_value=SearchDSLPatcher()):
            response = self.client.get(self.get_url())
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_limit_to_list_limit(self):
        self.create_admin_user()
        max_page_number = 1
        page_size = 1
        items = [Video(next(int_iterator)) for _ in range(max_page_number * page_size + 1)]
        VideoManager([Sections.GENERAL_DATA]).upsert(items)

        with patch.object(ResearchPaginator, "max_page_number",
                          new_callable=PropertyMock(return_value=max_page_number)):
            response = self.client.get(self.get_url(size=page_size))

            self.assertEqual(max_page_number, response.data["max_page"])
            self.assertEqual(page_size, len(response.data["items"]))
            self.assertEqual(len(items), response.data["items_count"])

    def test_extra_fields(self):
        self.create_admin_user()
        extra_fields = ("brand_safety_data", "chart_data", "transcript", "blacklist_data", "task_us_data",)
        extra_fields_map = {
            "task_us_data": "vetted_status"
        }
        video = Video(next(int_iterator))
        VideoManager([Sections.GENERAL_DATA]).upsert([video])

        url = self.get_url()
        response = self.client.get(url)

        first_item = response.data['items'][0]
        for field in extra_fields:
            with self.subTest(field):
                if field in extra_fields_map.keys():
                    self.assertIn(extra_fields_map.get(field), first_item)
                else:
                    self.assertIn(field, first_item)

    def test_filter_by_ids(self):
        self.create_admin_user()
        items_to_filter = 2
        videos = [Video(next(int_iterator)) for _ in range(items_to_filter + 1)]
        VideoManager([Sections.GENERAL_DATA]).upsert(videos)

        url = self.get_url(**{"main.id": ",".join([str(video.main.id) for video in videos[:items_to_filter]])})
        response = self.client.get(url)

        self.assertEqual(items_to_filter, len(response.data["items"]))

    def test_filter_by_single_id(self):
        self.create_admin_user()
        items_to_filter = 1
        videos = [Video(next(int_iterator)) for _ in range(items_to_filter + 1)]
        VideoManager([Sections.GENERAL_DATA]).upsert(videos)

        url = self.get_url(**{"main.id": videos[0].main.id})
        response = self.client.get(url)

        self.assertEqual(items_to_filter, len(response.data["items"]))

    def test_relevancy_score_sorting(self):
        """
        test that results are returned in the correct order when sorting by _score
        """
        self.create_admin_user()
        video_ids = [str(next(int_iterator)) for i in range(2)]
        most_relevant_video_title = "Herp derpsum herp derp sherper herp derp derpus herpus derpus"
        most_relevant_video = Video(**{
            "meta": {
                "id": video_ids[0],
            },
            "general_data": {
                "title": most_relevant_video_title,
                "description": "herp derper repper herpus"
            },
        })
        least_relevant_video = Video(**{
            "meta": {
                "id": video_ids[1],
            },
            "general_data": {
                "title": "Derp sherper perper tee. Derperker zerpus herpy derpus",
                "description": "reeper sherpus lurpy derps herp derp",
            },
        })
        VideoManager(sections=[Sections.GENERAL_DATA]).upsert([
            most_relevant_video,
            least_relevant_video
        ])

        search_term = "herp derp"
        desc_url = self.get_url() + urllib.parse.urlencode({
            "general_data.title": search_term,
            "general_data.description": search_term,
            "sort": "_score:desc",
        })
        desc_response = self.client.get(desc_url)
        desc_items = desc_response.data["items"]
        self.assertEqual(
            desc_items[0]["general_data"]["title"],
            most_relevant_video_title
        )

        asc_url = self.get_url() + urllib.parse.urlencode({
            "general_data.title": search_term,
            "general_data.description": search_term,
            "sort": "_score:asc",
        })
        asc_response = self.client.get(asc_url)
        asc_items = asc_response.data["items"]
        self.assertEqual(
            asc_items[-1]["general_data"]["title"],
            most_relevant_video_title
        )

    def test_video_id_query_param_mutation(self):
        """
        Test that a search on a video id correctly mutates the
        query params to return that video only, even where
        the search term exists in a field that is specified in
        the initial search
        """
        user = self.create_test_user()
        user.add_custom_user_permission("video_list")

        video_ids = [str(next(int_iterator)) for i in range(3)]
        video_one = Video(**{
            "meta": {"id": video_ids[0]},
            "main": {'id': video_ids[0]},
            "general_data": {
                "title": "video whose id we're searching for",
                "description": f"some description."
            }
        })
        video_two = Video(**{
            "meta": {"id": video_ids[1]},
            "main": {'id': video_ids[1]},
            "general_data": {
                "title": "the fox is quick",
                "description": f"some description. {video_ids[0]}"
            }
        })
        video_three = Video(**{
            "meta": {"id": video_ids[2]},
            "main": {'id': video_ids[2]},
            "general_data": {
                "title": "the fox is quick and brown",
                "description": f"some description. {video_ids[0]}"
            }
        })
        sections = [Sections.GENERAL_DATA, Sections.MAIN]
        VideoManager(sections=sections).upsert([video_one, video_two, video_three])

        search_term = video_ids[0]
        url = self.get_url() + urllib.parse.urlencode({
            "general_data.title": search_term,
            "general_data.description": search_term,
        })
        response = self.client.get(url)
        items = response.data['items']
        self.assertEqual(items[0]['main']['id'], video_ids[0])

    def test_vetted_status_field(self):
        self.create_admin_user()
        video_ids = []
        for i in range(6):
            video_ids.append(str(next(int_iterator)))

        videos = []
        videos.append(Video(**{
            "meta": {"id": video_ids[0]},
            "main": {'id': video_ids[0]},
            "general_data": {
                "title": f"video: {video_ids[0]}",
                "description": f"this video is vetted safe. Video id: {video_ids[0]}"
            },
            "task_us_data": {
                "last_vetted_at": timezone.now(),
                "brand_safety": [None,],
            },
        }))
        videos.append(Video(**{
            "meta": {"id": video_ids[1]},
            "main": {'id': video_ids[1]},
            "general_data": {
                "title": f"video: {video_ids[1]}",
                "description": f"this video is vetted safe. Video id: {video_ids[1]}"
            },
            "task_us_data": {
                "last_vetted_at": timezone.now(),
            },
        }))
        videos.append(Video(**{
            "meta": {"id": video_ids[2]},
            "main": {'id': video_ids[2]},
            "general_data": {
                "title": f"video: {video_ids[2]}",
                "description": f"this video is vetted risky. Video id: {video_ids[2]}"
            },
            "task_us_data": {
                "last_vetted_at": timezone.now(),
                "brand_safety": [1, 2, 3, 4],
            },
        }))
        # unvetted
        videos.append(Video(**{
            "meta": {"id": video_ids[3]},
            "main": {'id': video_ids[3]},
            "general_data": {
                "title": f"video: {video_ids[3]}",
                "description": f"this video is not vetted. Video id: {video_ids[3]}"
            },
            "task_us_data": {
                "brand_safety": [1,],
            },
        }))
        videos.append(Video(**{
            "meta": {"id": video_ids[4]},
            "main": {'id': video_ids[4]},
            "general_data": {
                "title": f"video: {video_ids[4]}",
                "description": f"this video is not vetted. Video id: {video_ids[4]}"
            },
            "task_us_data": {},
        }))
        videos.append(Video(**{
            "meta": {"id": video_ids[5]},
            "main": {'id': video_ids[5]},
            "general_data": {
                "title": f"video: {video_ids[5]}",
                "description": f"this video is not vetted. Video id: {video_ids[5]}"
            },
        }))

        VideoManager([Sections.GENERAL_DATA, Sections.MAIN, Sections.TASK_US_DATA]).upsert(videos)

        response = self.client.get(self.get_url())
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

        unvetted_url = self.get_url() + urllib.parse.urlencode({
            "task_us_data.last_vetted_at": False,
        })
        unvetted_response = self.client.get(unvetted_url)
        unvetted_items = unvetted_response.data['items']
        unvetted_video_ids = video_ids[3:]
        self.assertEqual([item['main']['id'] for item in unvetted_items].sort(), unvetted_video_ids.sort())

        vetted_url = self.get_url() + urllib.parse.urlencode({
            "task_us_data.last_vetted_at": True,
        })
        vetted_response = self.client.get(vetted_url)
        vetted_items = vetted_response.data['items']
        vetted_video_ids = video_ids[:3]
        self.assertEqual([item['main']['id'] for item in vetted_items].sort(), vetted_video_ids.sort())

    def test_permissions(self):
        user = self.create_test_user()
        Group.objects.get_or_create(name=PermissionGroupNames.BRAND_SAFETY_SCORING)
        user.add_custom_user_permission("video_list")

        video_id = str(next(int_iterator))
        video = Video(**{
            "meta": {"id": video_id},
            "main": {'id': video_id},
            "general_data": {
                "title": f"video: {video_id}",
                "description": f"this video is vetted safe. Video id: {video_id}"
            },
            "task_us_data": {
                "last_vetted_at": timezone.now(),
                "brand_safety": [None,],
            },
        })

        VideoManager([Sections.GENERAL_DATA, Sections.MAIN, Sections.TASK_US_DATA]).upsert([video])

        # normal user
        response = self.client.get(self.get_url())
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data['items']
        self.assertEqual(len(items), 1)
        item = items[0]
        item_fields = list(item.keys())
        self.assertNotIn("vetted_status", item_fields)
        self.assertNotIn("blacklist_data", item_fields)

        # audit vet admin
        Group.objects.get_or_create(name=PermissionGroupNames.AUDIT_VET_ADMIN)
        user.add_custom_user_permission("vet_audit_admin")
        response = self.client.get(self.get_url())
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data['items']
        self.assertEqual(len(items), 1)
        item = items[0]
        item_fields = list(item.keys())
        self.assertIn("vetted_status", item_fields)
        self.assertNotIn("blacklist_data", item_fields)

        # admin
        user.is_staff = True
        user.save()
        response = self.client.get(self.get_url())
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data['items']
        self.assertEqual(len(items), 1)
        item = items[0]
        item_fields = list(item.keys())
        self.assertIn("vetted_status", item_fields)
        self.assertIn("blacklist_data", item_fields)

    def test_vetting_admin_aggregations_guard(self):
        user = self.create_test_user()
        Group.objects.get_or_create(name=PermissionGroupNames.BRAND_SAFETY_SCORING)
        user.add_custom_user_permission("video_list")

        video_ids = []
        for i in range(2):
            video_ids.append(str(next(int_iterator)))
        videos = []
        # vetted
        videos.append(Video(**{
            "meta": {"id": video_ids[0]},
            "main": {'id': video_ids[0]},
            "general_data": {
                "title": f"video: {video_ids[0]}",
                "description": f"this video is vetted safe. Video id: {video_ids[0]}"
            },
            "task_us_data": {
                "last_vetted_at": timezone.now(),
                "brand_safety": [None,],
            },
        }))
        videos.append(Video(**{
            "meta": {"id": video_ids[1]},
            "main": {'id': video_ids[1]},
            "general_data": {
                "title": f"video: {video_ids[1]}",
                "description": f"this video is not vetted. Video id: {video_ids[1]}"
            },
        }))

        VideoManager([Sections.GENERAL_DATA, Sections.MAIN, Sections.TASK_US_DATA]).upsert(videos)

        url = self.get_url() + urlencode({
            "aggregations": ",".join(ALLOWED_VIDEO_AGGREGATIONS),
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
        user.is_staff = True
        user.save()
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        response_aggregations = response.data['aggregations']
        response_aggregation_keys = list(response_aggregations.keys())
        for aggregation in vetting_admin_aggregations:
            with self.subTest(aggregation):
                self.assertIn(aggregation, response_aggregation_keys)

        # vetting admin should see aggs
        user.is_staff = False
        user.save()
        Group.objects.get_or_create(name=PermissionGroupNames.AUDIT_VET_ADMIN)
        user.add_custom_user_permission("vet_audit_admin")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        response_aggregations = response.data['aggregations']
        response_aggregation_keys = list(response_aggregations.keys())
        for aggregation in vetting_admin_aggregations:
            with self.subTest(aggregation):
                self.assertIn(aggregation, response_aggregation_keys)

    def test_brand_safety_high_risk_permission(self):
        """
        test that a regular user can filter on RISKY or above scores, while
        admin users can additionally filter on HIGH_RISK scores
        """
        user = self.create_test_user()
        Group.objects.get_or_create(name=PermissionGroupNames.BRAND_SAFETY_SCORING)
        user.add_custom_user_permission("video_list")
        user.add_custom_user_group(PermissionGroupNames.BRAND_SAFETY_SCORING)

        video_id = str(next(int_iterator))
        video_id_2 = str(next(int_iterator))
        video = Video(**{
            "meta": {
                "id": video_id
            },
            "brand_safety": {
                "overall_score": 77
            }
        })
        video_2 = Video(**{
            "meta": {
                "id": video_id_2
            },
            "brand_safety": {
                "overall_score": 61
            }
        })
        sections = [Sections.GENERAL_DATA, Sections.MAIN, Sections.BRAND_SAFETY]
        VideoManager(sections=sections).upsert([video, video_2])
        url = self.get_url() + urlencode({
            "brand_safety": ",".join([constants.RISKY, constants.HIGH_RISK])
        })

        # regular user, no high risk allowed
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 1)

        # regular admin, all filters available
        user.is_staff = True
        user.save(update_fields=['is_staff'])
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 2)

        # vetting admin, all filters available
        user.is_staff = False
        user.save(update_fields=['is_staff'])
        Group.objects.get_or_create(name=PermissionGroupNames.AUDIT_VET_ADMIN)
        user.add_custom_user_permission("vet_audit_admin")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        items = response.data["items"]
        self.assertEqual(len(items), 2)

    def test_non_admin_brand_safety_exclusion(self):
        user = self.create_test_user()
        Group.objects.get_or_create(name=PermissionGroupNames.BRAND_SAFETY_SCORING)
        user.add_custom_user_permission("video_list")

        url = self.get_url() + urlencode({
            "aggregations": ",".join(ALLOWED_VIDEO_AGGREGATIONS),
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
        user.is_staff = True
        user.save(update_fields=["is_staff"])
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        response_aggregations = response.data["aggregations"]
        self.assertIn(constants.BRAND_SAFETY, list(response_aggregations.keys()))
        buckets = response_aggregations[constants.BRAND_SAFETY]["buckets"]
        self.assertEqual(len(buckets), 4)
        labels = [bucket['key'] for bucket in buckets]
        self.assertIn(constants.HIGH_RISK, labels)

        # vetting admin should see HIGH_RISK agg
        user.is_staff = False
        user.save(update_fields=["is_staff"])
        Group.objects.get_or_create(name=PermissionGroupNames.AUDIT_VET_ADMIN)
        user.add_custom_user_permission("vet_audit_admin")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        response_aggregations = response.data["aggregations"]
        self.assertIn(constants.BRAND_SAFETY, list(response_aggregations.keys()))
        buckets = response_aggregations[constants.BRAND_SAFETY]["buckets"]
        self.assertEqual(len(buckets), 4)
        labels = [bucket['key'] for bucket in buckets]
        self.assertIn(constants.HIGH_RISK, labels)

    def test_cache(self):
        """ Test subsequent requests uses cache """
        self.create_admin_user()
        flush_cache()
        url = self.get_url() + "?page=1&fields=main&sort=stats.views:desc"
        self.client.get(url)
        with patch("utils.es_components_cache.set_to_cache") as mock_set_cache, \
                override_settings(ES_CACHE_ENABLED=True):
            # Subsequent requests should use cache to retrieve but not set
            self.client.get(url)
        mock_set_cache.assert_not_called()
        flush_cache()

    def test_should_set_cache_threshold_expires(self):
        """ Test should_set_cache returns True only if page being requested is a default page and time to live expires """
        redis = get_redis_client()
        flush_cache()
        self.create_admin_user()
        url = self.get_url() + "?page=1&fields=main&sort=stats.views:desc"
        with override_settings(ES_CACHE_ENABLED=True):
            # Initial request to set cache
            self.client.get(url)
        # Manually update ttl for key to be below threshold to refresh cache
        cache_key = redis.keys(pattern="*get_data*")[0].decode("utf-8")
        redis.expire(cache_key, 0)
        # Cache is accessed twice for each get request, for a document count and a list of documents.
        # Use side effect to return first [0 count, 0 ttl] and [[] documents, 0 ttl]
        with patch("utils.es_components_cache.get_from_cache", side_effect=[(0, 0), ([], 0)]),\
            patch("utils.es_components_cache.set_to_cache") as mock_set_cache, \
                override_settings(ES_CACHE_ENABLED=True):

            # Normally this would retrieve cached data as the key ttl would still be valid.
            # However since redis.expire was used to manually reduce ttl, the cache should
            # be refreshed
            self.client.get(url)
        self.assertEqual(mock_set_cache.call_count, 2)
        flush_cache()

    def test_default_page_extended_timeout(self):
        """ Test that a default page uses an extended cache timeout e.g. First page of research with no filters """
        self.create_admin_user()
        url = self.get_url() + "page=1&fields=main&sort=stats.subscribers:desc"
        # Initial request to set cache
        with patch("utils.es_components_cache.set_to_cache") as mock_set_cache, \
                override_settings(ES_CACHE_ENABLED=True):
            self.client.get(url)
        args = mock_set_cache.call_args[1]
        self.assertEqual(args["timeout"], 14400)

    def test_relevancy_score_sorting_with_category_filter(self):
        """
        test that searching for results by relevancy (_score) asc/desc works
        when category filter is selected. Result items with matching
        primary categories should appear first with a +10 scoring boost when
        relevancy sorting is descending.
        """
        self.create_admin_user()
        video_ids = [str(next(int_iterator)) for i in range(2)]
        primary_category = "Music & Audio"
        least_relevant_video = Video(**{
            "meta": {
                "id": video_ids[1],
            },
            "general_data": {
                "iab_categories": ["Music & Audio", "Social"],
                "primary_category": "Social"
            },
        })
        most_relevant_video = Video(**{
            "meta": {
                "id": video_ids[0],
            },
            "general_data": {
                "iab_categories": ["Music & Audio", "Social"],
                "primary_category": primary_category
            },
        })
        VideoManager(sections=[Sections.GENERAL_DATA]).upsert([
            most_relevant_video,
            least_relevant_video
        ])

        desc_url = self.get_url() + urllib.parse.urlencode({
            "general_data.iab_categories": "Music & Audio",
            "sort": "_score:desc",
        })
        desc_response = self.client.get(desc_url)
        desc_items = desc_response.data["items"]
        self.assertEqual(
            desc_items[0]["general_data"]["primary_category"],
            primary_category
        )

        asc_url = self.get_url() + urllib.parse.urlencode({
            "general_data.iab_categories": "Music & Audio",
            "sort": "_score:asc",
        })
        asc_response = self.client.get(asc_url)
        asc_items = asc_response.data["items"]
        self.assertEqual(
            asc_items[-1]["general_data"]["primary_category"],
            primary_category
        )