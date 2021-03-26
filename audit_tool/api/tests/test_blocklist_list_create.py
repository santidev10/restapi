import json
from mock import patch

import time

from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.api.urls.names import AuditPathName
from audit_tool.models import BlacklistItem
from audit_tool.models import get_hash_name
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.models import Channel
from es_components.models import Video
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.patch_bulk_create import patch_bulk_create


class BlocklistListCreateTestCase(ExtendedAPITestCase, ESTestCase):
    SECTIONS = (Sections.BRAND_SAFETY, Sections.TASK_US_DATA, Sections.GENERAL_DATA, Sections.CUSTOM_PROPERTIES)
    channel_manager = ChannelManager(SECTIONS)
    video_manager = VideoManager(SECTIONS + (Sections.CHANNEL,))

    def setUp(self):
        self.user = self.create_test_user(perms={
            StaticPermissions.BLOCKLIST_MANAGER: True,
            StaticPermissions.BLOCKLIST_MANAGER__CREATE_CHANNEL: True,
            StaticPermissions.BLOCKLIST_MANAGER__DELETE_CHANNEL: True,
            StaticPermissions.BLOCKLIST_MANAGER__EXPORT_CHANNEL: True,
            StaticPermissions.BLOCKLIST_MANAGER__CREATE_VIDEO: True,
            StaticPermissions.BLOCKLIST_MANAGER__DELETE_VIDEO: True,
            StaticPermissions.BLOCKLIST_MANAGER__EXPORT_VIDEO: True,
        })

    def _get_url(self, data_type):
        url = reverse(AuditPathName.BLOCKLIST_LIST_CREATE, [Namespace.AUDIT_TOOL], kwargs=dict(data_type=data_type))
        return url

    def _get_youtube_url(self, item_id, data_type="video"):
        resource = "/watch?v=" if data_type == "video" else "/channel/"
        return f"https://www.youtube.com{resource}{item_id}"

    def _create_doc(self, data_type="video", blocklist=True):
        if data_type == "video":
            id_length = 11
            model = Video
            suffix = "_video"
        else:
            id_length = 24
            model = Channel
            suffix = "_channel"
        doc_id = (str(next(int_iterator)) + suffix).zfill(id_length)
        doc = model(doc_id)
        doc.populate_general_data(title=f"Title for {doc.main.id}")
        doc.populate_custom_properties(blocklist=blocklist)
        return doc

    def test_admin_permission(self):
        """ Test only admin access """
        self.create_test_user()
        res1 = self.client.post(self._get_url("video") + "?block=true", data=json.dumps({}), content_type="application/json")
        res2 = self.client.post(self._get_url("channel") + "?block=false", data=json.dumps({}),
                                content_type="application/json")
        self.assertEqual(res1.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(res2.status_code, HTTP_403_FORBIDDEN)

    def test_add_increments(self):
        """ Test that new and existing items to blacklist increments blocked count """
        user = self.user
        videos = [self._create_doc("video") for _ in range(2)]
        channels = [self._create_doc("channel") for _ in range(2)]

        self.video_manager.upsert(videos)
        self.channel_manager.upsert(channels)

        bl_v_exists = BlacklistItem\
            .objects\
            .create(item_id=videos[0].main.id, item_type=0, blocked_count=1,
                    item_id_hash=get_hash_name(videos[1].main.id), processed_by_user_id=user.id, blocklist=False)
        bl_c_exists = BlacklistItem\
            .objects\
            .create(item_id=channels[0].main.id, item_type=1, blocked_count=4,
                    item_id_hash=get_hash_name(channels[1].main.id), processed_by_user_id=user.id, blocklist=False)
        # Also test view can handle just sending ids instead of full urls
        payload1 = dict(item_urls=[i.main.id for i in videos])
        payload2 = dict(item_urls=[i.main.id for i in channels])
        with patch("audit_tool.api.views.blocklist.blocklist_list_create.safe_bulk_create", new=patch_bulk_create):
            res1 = self.client.post(self._get_url("video") + "?block=true", data=json.dumps(payload1), content_type="application/json")
            res2 = self.client.post(self._get_url("channel") + "?block=true", data=json.dumps(payload2), content_type="application/json")

        self.assertEqual(res1.status_code, HTTP_200_OK)
        self.assertEqual(res2.status_code, HTTP_200_OK)

        blv1, blv2 = BlacklistItem.objects.filter(item_id__in=payload1["item_urls"], item_type=0).order_by("id")
        blc1, blc2 = BlacklistItem.objects.filter(item_id__in=payload2["item_urls"], item_type=1).order_by("id")

        # blocked_count should increment by 1 from existing value only if value is changing
        self.assertEqual(blv1.blocked_count, bl_v_exists.blocked_count + 1)
        self.assertEqual(blv1.unblocked_count, 0)
        self.assertEqual(blv1.processed_by_user_id, user.id)
        self.assertEqual(blv1.blocklist, True)

        self.assertEqual(blv2.blocked_count, 1)
        self.assertEqual(blv2.unblocked_count, 0)
        self.assertEqual(blv2.processed_by_user_id, user.id)
        self.assertEqual(blv2.blocklist, True)

        self.assertEqual(blc1.blocked_count, bl_c_exists.blocked_count + 1)
        self.assertEqual(blc1.unblocked_count, 0)
        self.assertEqual(blc1.processed_by_user_id, user.id)
        self.assertEqual(blc1.blocklist, True)

        self.assertEqual(blc2.blocked_count, 1)
        self.assertEqual(blc2.unblocked_count, 0)
        self.assertEqual(blc2.processed_by_user_id, user.id)
        self.assertEqual(blc2.blocklist, True)

    def test_remove_increments(self):
        """ Test that removing only existing items from blocklist increments unblocked count """
        user = self.user
        videos = [self._create_doc("video") for _ in range(2)]
        channels = [self._create_doc("channel") for _ in range(2)]
        bl_v_exists = BlacklistItem.objects.create(item_id=videos[0].main.id, item_type=0, unblocked_count=11,
                                                   item_id_hash=get_hash_name(videos[1].main.id), processed_by_user_id=user.id)
        bl_c_exists = BlacklistItem.objects.create(item_id=channels[0].main.id, item_type=1, unblocked_count=9,
                                                   item_id_hash=get_hash_name(channels[1].main.id), processed_by_user_id=user.id)
        # Also test view can handle just sending ids instead of full urls
        payload1 = dict(item_urls=[i.main.id for i in videos])
        payload2 = dict(item_urls=[i.main.id for i in channels])
        with patch("audit_tool.api.views.blocklist.blocklist_list_create.safe_bulk_create", new=patch_bulk_create):
            res1 = self.client.post(self._get_url("video") + "?block=false", data=json.dumps(payload1),
                                    content_type="application/json")
            res2 = self.client.post(self._get_url("channel") + "?block=false", data=json.dumps(payload2),
                                    content_type="application/json")
        self.assertEqual(res1.status_code, HTTP_200_OK)
        self.assertEqual(res2.status_code, HTTP_200_OK)

        # Should only be one item as items that are being unblocked but are not on the blocklist should not have an
        # entry created
        blv = BlacklistItem.objects.get(item_id=payload1["item_urls"][0], item_type=0)
        blc = BlacklistItem.objects.get(item_id=payload2["item_urls"][0], item_type=1)
        # unblocked_count should increment by 1 from existing value only if value is changing
        self.assertEqual(blv.unblocked_count, bl_v_exists.unblocked_count + 1)
        self.assertEqual(blv.blocked_count, 0)
        self.assertEqual(blv.processed_by_user_id, user.id)
        self.assertEqual(blv.blocklist, False)

        self.assertEqual(blc.unblocked_count, bl_c_exists.unblocked_count + 1)
        self.assertEqual(blc.blocked_count, 0)
        self.assertEqual(blc.processed_by_user_id, user.id)
        self.assertEqual(blc.blocklist, False)

    def test_unblock_rescore(self):
        """ Test that unblocking items sets brand safety rescore field to true """
        video = self._create_doc("video")
        channel = self._create_doc("channel")

        self.video_manager.upsert([video])
        self.channel_manager.upsert([channel])
        payload1 = json.dumps(dict(item_urls=[self._get_youtube_url(video.main.id)]))
        payload2 = json.dumps(dict(item_urls=[self._get_youtube_url(channel.main.id, "channel")]))
        with patch("audit_tool.api.views.blocklist.blocklist_list_create.safe_bulk_create", new=patch_bulk_create):
            res1 = self.client.post(self._get_url("video") + "?block=false", data=payload1, content_type="application/json")
            res2 = self.client.post(self._get_url("channel") + "?block=false", data=payload2, content_type="application/json")

        self.assertEqual(res1.status_code, HTTP_200_OK)
        self.assertEqual(res2.status_code, HTTP_200_OK)

        updated_video = self.video_manager.get([video.main.id], skip_none=True)[0]
        updated_channel = self.channel_manager.get([channel.main.id], skip_none=True)[0]
        self.assertEqual(updated_video.brand_safety.rescore, True)
        self.assertEqual(updated_channel.brand_safety.rescore, True)

    def test_block_zero_score(self):
        """ Test that blocking items sets brand safety overall score to 0 """
        video = self._create_doc("video")
        channel = self._create_doc("channel")
        video.populate_brand_safety(overall_score=100)
        channel.populate_brand_safety(overall_score=100)

        self.video_manager.upsert([video])
        self.channel_manager.upsert([channel])
        payload1 = json.dumps(dict(item_urls=[self._get_youtube_url(video.main.id)]))
        payload2 = json.dumps(dict(item_urls=[self._get_youtube_url(channel.main.id, "channel")]))
        with patch("audit_tool.api.views.blocklist.blocklist_list_create.safe_bulk_create", new=patch_bulk_create):
            res1 = self.client.post(self._get_url("video") + "?block=true", data=payload1, content_type="application/json")
            res2 = self.client.post(self._get_url("channel") + "?block=true", data=payload2, content_type="application/json")

        self.assertEqual(res1.status_code, HTTP_200_OK)
        self.assertEqual(res2.status_code, HTTP_200_OK)

        updated_video = self.video_manager.get([video.main.id], skip_none=True)[0]
        updated_channel = self.channel_manager.get([channel.main.id], skip_none=True)[0]
        self.assertEqual(updated_video.brand_safety.overall_score, 0)
        self.assertEqual(updated_channel.brand_safety.overall_score, 0)
        self.assertEqual(updated_video.brand_safety.rescore, False)
        self.assertEqual(updated_channel.brand_safety.rescore, False)

        with self.subTest("Test blocking videos with /videos/ resource url"):
            video = self._create_doc("video")
            video.populate_brand_safety(overall_score=100)
            payload = json.dumps(dict(item_urls=[f"https://www.youtube.com/video/{video.main.id}"]))
            with patch("audit_tool.api.views.blocklist.blocklist_list_create.safe_bulk_create", new=patch_bulk_create):
                res = self.client.post(self._get_url("video") + "?block=true", data=payload,
                                        content_type="application/json")
            self.assertEqual(res.status_code, HTTP_200_OK)
            updated_video = self.video_manager.get([video.main.id], skip_none=True)[0]
            self.assertEqual(updated_video.brand_safety.overall_score, 0)
            self.assertEqual(updated_video.brand_safety.rescore, False)

    def test_list_search(self):
        """ Test search by id and title """
        user = self.user
        video_target = self._create_doc("video")
        video_target.populate_general_data(title="Main video")
        video_target.populate_custom_properties(blocklist=True)
        channel_target = self._create_doc("channel")
        channel_target.populate_general_data(title="Focus channel")
        channel_target.populate_custom_properties(blocklist=True)
        other_videos = [self._create_doc("video") for _ in range(10)]
        other_channels = [self._create_doc("video") for _ in range(10)]

        blv = BlacklistItem.objects.create(item_id=video_target.main.id, item_type=0,
                                           item_id_hash=get_hash_name(video_target.main.id), blocked_count=3,
                                           unblocked_count=8, processed_by_user_id=user.id)
        blc = BlacklistItem.objects.create(item_id=channel_target.main.id, item_type=1,
                                           item_id_hash=get_hash_name(channel_target.main.id), blocked_count=11,
                                           unblocked_count=35, processed_by_user_id=user.id)

        self.video_manager.upsert([video_target, *other_videos])
        self.channel_manager.upsert([channel_target, *other_channels])

        resv1 = self.client.get(self._get_url("video") + "?search=Main")
        resv2 = self.client.get(self._get_url("video") + "?search=" + video_target.main.id)
        datav1 = resv1.data
        datav2 = resv2.data
        v1, v2 = resv1.data["items"][0], resv2.data["items"][0]
        # Response from searching by title or id should be same
        # Creating set from response values should equal existing item
        self.assertEqual(resv1.status_code, HTTP_200_OK)
        self.assertEqual({datav1["items_count"], datav2["items_count"]}, {1})
        self.assertEqual({v1["title"], v2["title"]}, {video_target.general_data.title})
        self.assertEqual({v1["blocked_count"], v2["blocked_count"]}, {blv.blocked_count})
        self.assertEqual({v1["unblocked_count"], v2["unblocked_count"]}, {blv.unblocked_count})
        self.assertEqual({v1["added_by_user"], v2["added_by_user"]}, {f"{user.first_name} {user.last_name}"})

        resc1 = self.client.get(self._get_url("channel") + "?search=Focus")
        resc2 = self.client.get(self._get_url("channel") + "?search=" + channel_target.main.id)
        datac1 = resc1.data
        datac2 = resc2.data
        c1, c2 = resc1.data["items"][0], resc2.data["items"][0]
        # Response from searching by title or id should be same
        self.assertEqual(resc1.status_code, HTTP_200_OK)
        self.assertEqual({datac1["items_count"], datac2["items_count"]}, {1})
        self.assertEqual({c1["title"], c2["title"]}, {channel_target.general_data.title})
        self.assertEqual({c1["blocked_count"], c2["blocked_count"]}, {blc.blocked_count})
        self.assertEqual({c1["unblocked_count"], c2["unblocked_count"]}, {blc.unblocked_count})
        self.assertEqual({c1["added_by_user"], c2["added_by_user"]}, {f"{user.first_name} {user.last_name}"})

    def test_does_not_update_same_blocklist_value(self):
        """ Should not update BlacklistItem object if blocklist value does not change """
        video = self._create_doc("video")
        channel = self._create_doc("channel")
        video.populate_custom_properties(blocklist=True)
        video.populate_custom_properties(blocklist=True)

        bl1 = BlacklistItem.objects.create(item_id=video.main.id, item_type=0, item_id_hash=get_hash_name(video.main.id),
                                           blocked_count=1, unblocked_count=1)
        bl2 = BlacklistItem.objects.create(item_id=channel.main.id, item_type=1,
                                           item_id_hash=get_hash_name(channel.main.id),
                                           blocked_count=1, unblocked_count=1)
        self.video_manager.upsert([video])
        self.channel_manager.upsert([channel])

        payload1 = {"item_ids": [video.main.id]}
        payload2 = {"item_ids": [channel.main.id]}
        with patch("audit_tool.api.views.blocklist.blocklist_list_create.safe_bulk_create", new=patch_bulk_create):
            res1 = self.client.post(self._get_url("video") + "?block=true", data=json.dumps(payload1),
                                    content_type="application/json")
            res2 = self.client.post(self._get_url("channel") + "?block=true", data=json.dumps(payload2),
                                    content_type="application/json")

        self.assertEqual(res1.status_code, HTTP_200_OK)
        self.assertEqual(res2.status_code, HTTP_200_OK)

        updated_video = self.video_manager.get([video.main.id])[0]
        updated_channel = self.channel_manager.get([channel.main.id])[0]
        updated_bl1 = BlacklistItem.objects.get(item_id=video.main.id)
        updated_bl2 = BlacklistItem.objects.get(item_id=channel.main.id)

        self.assertEqual(video.custom_properties.blocklist, updated_video.custom_properties.blocklist)
        self.assertEqual(channel.custom_properties.blocklist, updated_channel.custom_properties.blocklist)
        self.assertEqual(bl1.blocked_count, updated_bl1.blocked_count)
        self.assertEqual(bl1.unblocked_count, updated_bl1.unblocked_count)
        self.assertEqual(bl2.blocked_count, updated_bl2.blocked_count)
        self.assertEqual(bl2.unblocked_count, updated_bl2.unblocked_count)

    def test_channel_block_videos_block(self):
        """
        Test blocklisting channels does not set blocklist values on videos. Videos are implicitly blocklisted
        from channel blocklist value
        https://channelfactory.atlassian.net/browse/VIQ2-488
        """
        channel1 = self._create_doc("channel")
        channel2 = self._create_doc("channel")

        videos1 = [self._create_doc("video", blocklist=False) for _ in range(2)]
        for v in videos1:
            v.populate_channel(id=channel1.main.id)
        videos2 = [self._create_doc("video", blocklist=False) for _ in range(2)]
        for v in videos2:
            v.populate_channel(id=channel2.main.id)

        channels = [channel1, channel2]
        self.video_manager.upsert(videos1 + videos2)
        self.channel_manager.upsert(channels)
        payload = json.dumps(dict(item_urls=[self._get_youtube_url(channel.main.id, "channel") for channel in channels]))
        with patch("audit_tool.api.views.blocklist.blocklist_list_create.safe_bulk_create", new=patch_bulk_create):
            res = self.client.post(self._get_url("channel") + "?block=true", data=payload,
                                    content_type="application/json")

        self.assertEqual(res.status_code, HTTP_200_OK)
        updated_videos = self.video_manager.get([video.main.id for video in videos1 + videos2], skip_none=True)
        updated_channels = self.channel_manager.get([channel.main.id for channel in channels], skip_none=True)

        self.assertTrue(all(item.custom_properties.blocklist is True for item in updated_channels))
        self.assertTrue(all(item.custom_properties.blocklist is not True for item in updated_videos))

    def test_channel_unblock(self):
        """ Test blocklisting channels does not unblock already blocked videos"""
        channel1 = self._create_doc("channel")
        channel2 = self._create_doc("channel")
        channel3 = self._create_doc("channel")
        blocklist = {"custom_properties": {"blocklist": True}}
        videos1 = [
            Video(
                **{"main": {"id": f"video_{next(int_iterator)}"}, "channel": {"id": channel1.main.id}},
                **blocklist
            ) for _ in range(2)]
        videos2 = [
            Video(
                **{"main": {"id": f"video_{next(int_iterator)}"}, "channel": {"id": channel2.main.id}},
                **blocklist
            ) for _ in range(2)]
        channels = [channel1, channel2, channel3]
        self.video_manager.upsert(videos1 + videos2)
        self.channel_manager.upsert(channels)
        payload = json.dumps(dict(item_urls=[self._get_youtube_url(channel.main.id, "channel") for channel in channels]))
        with patch("audit_tool.api.views.blocklist.blocklist_list_create.safe_bulk_create", new=patch_bulk_create):
            res = self.client.post(self._get_url("channel") + "?block=false", data=payload,
                                    content_type="application/json")

        self.assertEqual(res.status_code, HTTP_200_OK)
        updated_videos = self.video_manager.get([video.main.id for video in videos1 + videos2], skip_none=True)
        updated_channels = self.channel_manager.get([channel.main.id for channel in channels], skip_none=True)

        self.assertTrue(all(item.custom_properties.blocklist is False for item in updated_channels))
        self.assertTrue(all(item.custom_properties.blocklist is True for item in updated_videos))


    def test_blocklist_create_delete_permissions(self):
        self.create_test_user(perms={
            StaticPermissions.BLOCKLIST_MANAGER: True,
            StaticPermissions.BLOCKLIST_MANAGER__CREATE_CHANNEL: False,
            StaticPermissions.BLOCKLIST_MANAGER__DELETE_CHANNEL: False,
            StaticPermissions.BLOCKLIST_MANAGER__CREATE_VIDEO: False,
            StaticPermissions.BLOCKLIST_MANAGER__DELETE_VIDEO: False,
        })

        res1 = self.client.post(self._get_url("video") + "?block=true", data=json.dumps({}),
                                content_type="application/json")
        res2 = self.client.post(self._get_url("video") + "?block=false", data=json.dumps({}),
                                content_type="application/json")
        res3 = self.client.post(self._get_url("channel") + "?block=true", data=json.dumps({}),
                                content_type="application/json")
        res4 = self.client.post(self._get_url("channel") + "?block=false", data=json.dumps({}),
                                content_type="application/json")
        self.assertEqual(res1.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(res2.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(res3.status_code, HTTP_403_FORBIDDEN)
        self.assertEqual(res4.status_code, HTTP_403_FORBIDDEN)

    def test_blocklist_list_updated_at_sort(self):
        self.create_test_user(perms={
            StaticPermissions.BLOCKLIST_MANAGER: True
        })

        video_1 = self._create_doc("video")
        video_2 = self._create_doc("video")
        channel_1 = self._create_doc("channel")
        channel_2 = self._create_doc("channel")
        video_1.populate_custom_properties(blocklist=True)
        video_2.populate_custom_properties(blocklist=True)
        channel_1.populate_custom_properties(blocklist=True)
        channel_2.populate_custom_properties(blocklist=True)

        self.video_manager.upsert([video_1, video_2])
        self.channel_manager.upsert([channel_1, channel_2])

        video_bl1 = BlacklistItem.objects.create(item_id=video_1.main.id, item_type=0,
                                           item_id_hash=get_hash_name(video_1.main.id),
                                           blocked_count=1, unblocked_count=1)
        video_bl2 = BlacklistItem.objects.create(item_id=video_2.main.id, item_type=0,
                                           item_id_hash=get_hash_name(video_2.main.id),
                                           blocked_count=1, unblocked_count=1)
        channel_bl1 = BlacklistItem.objects.create(item_id=channel_1.main.id, item_type=1,
                                           item_id_hash=get_hash_name(channel_1.main.id),
                                           blocked_count=1, unblocked_count=1)
        channel_bl2 = BlacklistItem.objects.create(item_id=channel_2.main.id, item_type=1,
                                           item_id_hash=get_hash_name(channel_2.main.id),
                                           blocked_count=1, unblocked_count=1)
        video_bl1.save()
        time.sleep(1)
        video_bl2.save()

        channel_bl1.save()
        time.sleep(1)
        channel_bl2.save()

        res_v = self.client.get(self._get_url("video"))
        res_ch = self.client.get(self._get_url("channel"))

        v1, v2 = res_v.data["items"][0], res_v.data["items"][1]
        c1, c2 = res_ch.data["items"][0], res_ch.data["items"][1]

        self.assertEqual(v1['url'].split("/watch?v=")[-1], video_bl2.item_id)
        self.assertEqual(v2['url'].split("/watch?v=")[-1], video_bl1.item_id)
        self.assertEqual(c1['url'].split("/channel/")[-1], channel_bl2.item_id)
        self.assertEqual(c2['url'].split("/channel/")[-1], channel_bl1.item_id)
