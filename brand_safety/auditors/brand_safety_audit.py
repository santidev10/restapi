from collections import Counter
from collections import defaultdict
from datetime import datetime
import logging
import multiprocessing as mp
import time

import pytz

from audit_tool.models import BlacklistItem
from brand_safety.constants import BRAND_SAFETY_SCORE
from brand_safety.constants import BLACKLIST_DATA
from brand_safety.audit_models.brand_safety_channel_audit import BrandSafetyChannelAudit
from brand_safety.audit_models.brand_safety_video_audit import BrandSafetyVideoAudit
from brand_safety.auditors.serializers import BrandSafetyChannelSerializer
from brand_safety.auditors.serializers import BrandSafetyVideoSerializer
from brand_safety.auditors.utils import AuditUtils
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.constants import VIDEO_CHANNEL_ID_FIELD
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder

logger = logging.getLogger(__name__)


class BrandSafetyAudit(object):
    """
    Interface for reading source data and providing it to services
    """
    MAX_POOL_COUNT = None
    CHANNEL_POOL_BATCH_SIZE = None
    CHANNEL_MASTER_BATCH_SIZE = None
    # Hours in which a channel should be updated
    UPDATE_TIME_THRESHOLD = 24 * 3
    CHANNEL_BATCH_COUNTER_LIMIT = 500
    ES_LIMIT = 10000
    MINIMUM_SUBSCRIBER_COUNT = 1000
    SLEEP = 2
    channel_batch_counter = 1

    def __init__(self, *_, **kwargs):
        # If initialized with an APIScriptTracker instance, then expected to run full brand safety
        # else main run method should not be called since it relies on an APIScriptTracker instance
        try:
            self.script_tracker = kwargs["api_tracker"]
            self.cursor_id = self.script_tracker.cursor_id
            self.is_manual = False
        except KeyError:
            self.is_manual = True
        if kwargs["discovery"]:
            self.discovery = True
            self._set_discovery_config()
        else:
            self.discovery = False
            self._set_update_config()
        self.audit_utils = AuditUtils()
        self.channel_manager = ChannelManager(
            sections=(Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS, Sections.BRAND_SAFETY),
            upsert_sections=(Sections.BRAND_SAFETY,)
        )
        self.video_manager = VideoManager(
            sections=(Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS, Sections.CHANNEL, Sections.BRAND_SAFETY, Sections.CAPTIONS),
            upsert_sections=(Sections.BRAND_SAFETY,)
        )

    def _set_discovery_config(self):
        self.MAX_POOL_COUNT = 1
        self.CHANNEL_POOL_BATCH_SIZE = 1
        self.CHANNEL_MASTER_BATCH_SIZE = self.MAX_POOL_COUNT * self.CHANNEL_POOL_BATCH_SIZE

    def _set_update_config(self):
        self.MAX_POOL_COUNT = 2
        self.CHANNEL_POOL_BATCH_SIZE = 10
        self.CHANNEL_MASTER_BATCH_SIZE = self.MAX_POOL_COUNT * self.CHANNEL_POOL_BATCH_SIZE

    def _set_manual_config(self):
        self.MAX_POOL_COUNT = 5
        self.CHANNEL_POOL_BATCH_SIZE = 10
        self.CHANNEL_MASTER_BATCH_SIZE = self.MAX_POOL_COUNT * self.CHANNEL_POOL_BATCH_SIZE

    def run(self):
        """
        Pools processes to handle main audit logic and processes results
            If initialized with an APIScriptTracker instance, then expected to run full brand safety
                else main run method should not be called since it relies on an APIScriptTracker instance
        :return: None
        """
        if self.is_manual:
            raise ValueError("Provider was not initialized with an APIScriptTracker instance.")
        pool = mp.Pool(processes=self.MAX_POOL_COUNT)
        for channel_batch in self._channel_generator(self.cursor_id):
            # Some batches may be empty if none of the channels retrieved have full data to be audited
            # _channel_generator will stop when no items are retrieved from Elasticsearch
            if not channel_batch:
                continue
            results = self._process_audits(channel_batch)
            # results = pool.map(self._process_audits,
            #                    self.audit_utils.batch(channel_batch, self.CHANNEL_POOL_BATCH_SIZE))
            # Extract nested results from each process and index into es
            video_audits, channel_audits = self._extract_results(results)
            # Index items
            self._index_results(video_audits, channel_audits)

            if self.channel_batch_counter % 10 == 0:
                # Update config in case they have been modified
                self.audit_utils.update_config()
        logger.error("Complete.")

    def _process_audits(self, channels: list) -> dict:
        """
        Drives main brand safety logic for each process
        :param channels: Channel documents with videos to retrieve video data for
        :return:
        """
        results = {
            "video_audits": [],
            "channel_audits": []
        }
        channel_ids = [item["id"] for item in channels]
        channel_blacklist_data_ref = {
            item.item_id: item.blacklist_category
            for item in BlacklistItem.get(channel_ids, 1)
        }
        for channel in channels:
            video_audits = self.audit_videos(videos=channel["videos"])
            channel["video_audits"] = video_audits

            channel_blacklist_data = channel_blacklist_data_ref.get(channel["id"], {})
            channel_audit = self.audit_channel(channel, blacklist_data=channel_blacklist_data)

            results["video_audits"].extend(video_audits)
            results["channel_audits"].append(channel_audit)
        return results

    def audit_video(self, video_data: dict, blacklist_data=None, full_audit=True) -> BrandSafetyVideoAudit:
        """
        Audit single video
        :param video_data: dict -> Data to audit
            Required keys: video_id, title
            Optional keys: description, tags, transcript
        :return:
        """
        if blacklist_data is None:
            try:
                blacklist_data = BlacklistItem.get(video_data["id"], 0)[0].catgories
            except (IndexError, AttributeError):
                pass
        # Every audit should have language_processors in config
        audit = BrandSafetyVideoAudit(
            video_data,
            self.audit_utils,
            blacklist_data
        )
        audit.run()
        if not full_audit:
            audit = getattr(audit, BRAND_SAFETY_SCORE).overall_score
        return audit

    def audit_videos(self, channels=None, videos=None):
        """
        Audits videos with blacklist data
            Videos with blacklist data will their blacklisted category scores set to zero
        :param channels: list (dict) -> Channels to audit videos for
        :param videos: list (dict) -> Data to provide to BrandSafetyVideoAudit
        :return: list (int | BrandSafetyVideoAudit) ->
            full_audit=False: (int) BrandSafetyVideoAudit score
            full_audit=True: BrandSafetyVideoAudit object
        """
        # Set defaults here to get access to self
        video_audits = []
        if videos and channels:
            raise ValueError("You must either provide video data to audit or channels to retrieve video data for.")
        if videos is None:
            videos = self._get_channel_videos(channels)

        video_ids = [item["id"] for item in videos]
        blacklist_data_ref = {
            item.item_id: item.blacklist_category
            for item in BlacklistItem.get(video_ids, 0)
        }

        for video in videos:
            try:
                video = video.to_dict()
            except AttributeError:
                pass
            try:
                blacklist_data = blacklist_data_ref.get(video["id"])
                audit = self.audit_video(video, blacklist_data=blacklist_data)
                video_audits.append(audit)
            except KeyError as e:
                # Ignore videos without full data
                continue
        return video_audits

    def audit_channel(self, channel_data, full_audit=True, blacklist_data=None):
        """
        Audit single channel
        :param channel_data: dict -> Data to audit
            Required keys: channel_id, title
            Optional keys: description, video_tags
        :return:
        """
        if blacklist_data is None:
            try:
                blacklist_data = BlacklistItem.get(channel_data["id"], 1)[0].categories
            except (IndexError, AttributeError):
                pass
        audit = BrandSafetyChannelAudit(channel_data, self.audit_utils, blacklist_data)
        audit.run()
        if not full_audit:
            audit = getattr(audit, BRAND_SAFETY_SCORE).overall_score
        return audit

    def audit_channels(self, channel_video_audits: dict = None) -> list:
        """
        Audits Channels by retrieving channel data and using sorted Video audit objects by channel id
        :param channel_video_audits: key: Channel id, value: BrandSafetyVideoAudit objects
        :return: list -> BrandSafetyChannelAudit Audit objects
        """
        channel_audits = []
        for _id, data in channel_video_audits.items():
            # Don't score channels without videos
            if data.get("video_audits") is None:
                continue
            try:
                audit = self.audit_channel(data)
                channel_audits.append(audit)
            except KeyError as e:
                # Ignore channels without full data
                continue
        return channel_audits

    def _get_channel_videos(self, channel_ids: list) -> list:
        """
        Get videos for channels
        :param channels: dict -> channel_id, channel_metadata
        :return:
        """
        all_results = []
        for batch in self.audit_utils.batch(channel_ids, 3):
            query = QueryBuilder().build().must().terms().field(VIDEO_CHANNEL_ID_FIELD).value(batch).get()
            results = self.video_manager.search(query, limit=self.ES_LIMIT).execute().hits
            all_results.extend(results)
        data = BrandSafetyVideoSerializer(all_results, many=True).data
        return data

    def _extract_results(self, results: list):
        """
        Extracts nested results from each of the processes
        :param results: list -> Dict results from each process
        :return:
        """
        video_audits = []
        channel_audits = []
        for batch in results:
            video_audits.extend(batch["video_audits"])
            channel_audits.extend(batch["channel_audits"])
        return video_audits, channel_audits

    def _index_results(self, video_audits, channel_audits):
        """
        Upsert documents with brand safety data
        :param video_audits: list -> BrandSafetyVideo audits
        :param channel_audits: list -> BrandSafetyChannel audits
        :return:
        """
        videos = [audit.instantiate_es() for audit in video_audits]
        channels = [audit.instantiate_es() for audit in channel_audits]
        self.channel_manager.upsert(channels)
        self.video_manager.upsert(videos)

    def _channel_generator(self, cursor_id):
        """
        Get channels to score with no brand safety data
        :param cursor_id:
        :return:
        """
        cursor_id = cursor_id or ""
        while True:
            query = QueryBuilder().build().must().term().field(MAIN_ID_FIELD).value("UCSyBSyLk4ZvXd2Fj-tLojag").get()
            # query = QueryBuilder().build().must().range().field(MAIN_ID_FIELD).gte(cursor_id).get()
            # query &= QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get()
            # query &= QueryBuilder().build().must().range().field("stats.subscribers").gte(
            #     self.MINIMUM_SUBSCRIBER_COUNT).get()
            response = self.channel_manager.search(query, limit=self.CHANNEL_MASTER_BATCH_SIZE, sort=("-stats.subscribers",)).execute()
            results = response.hits

            if not results:
                self.audit_utils.set_cursor(self.script_tracker, None, integer=False)
                break

            channels = BrandSafetyChannelSerializer(results, many=True).data
            if not self.discovery:
                channels = self._get_channels_to_update(channels, check_last_updated=True)
            else:
                channels = self._get_channels_to_update(channels, check_last_updated=False)
            yield channels

            cursor_id = results[-1].main.id
            self.script_tracker = self.audit_utils.set_cursor(self.script_tracker, cursor_id, integer=False)
            self.cursor_id = self.script_tracker.cursor_id
            self.channel_batch_counter += 1

    def _get_channels_to_update(self, channel_batch, check_last_updated=False):
        """
        Gets channels to update
            If either the last time the channel has been updated is greater than threshold time or if the number of
            videos has changed since the last time the channel was scored, it should be updated
        :param channel_batch: list
        :return: list
        """
        channels = {
            item["id"]: item for item in channel_batch
        }
        channels_to_update = []
        # Get videos for channels
        videos = self._get_channel_videos(list(channels.keys()))
        videos_by_channel = defaultdict(list)
        for video in videos:
            if not video.get("channel_id"):
                continue
            videos_by_channel[video["channel_id"]].append(video)
        # Get counts of videos for each channel
        channel_video_counts = Counter([item["channel_id"] for item in videos if item.get("channel_id")])

        # For each channel retrieved in original query, check if it should be updated
        for _id, data in channels.items():
            should_update = False
            if not check_last_updated or not data["updated_at"] or not data["videos_scored"]:
                should_update = True
            else:
                hours_elapsed = (datetime.now(pytz.utc) - data["updated_at"]).seconds // 3600
                # If last time channel was updated is greater than threshold or number of channel's videos has changed, rescore
                if hours_elapsed >= self.UPDATE_TIME_THRESHOLD or data["videos_scored"] != channel_video_counts[_id]:
                    should_update = True

            if should_update:
                data["videos"] = list(videos_by_channel.get(_id, []))
                channels_to_update.append(data)
        return list(channels_to_update)

    def manual_channel_audit(self, channel_ids: iter):
        """
        Score specific channels and videos
        :param channel_ids: list | tuple
        :return: None
        """
        to_audit = []
        channels = self.audit_utils.get_items(channel_ids, self.channel_manager)
        for item in channels:
            data = BrandSafetyChannelSerializer(item).data
            data["videos"] = self._get_channel_videos([data["id"]])
        pool = mp.Pool(processes=self.MAX_POOL_COUNT)
        results = pool.map(self._process_audits, self.audit_utils.batch(to_audit, self.CHANNEL_POOL_BATCH_SIZE))

        # Extract nested results from each process and index into es
        video_audits, channel_audits = self._extract_results(results)
        self._index_results(video_audits, channel_audits)
        return channel_audits

    def manual_video_audit(self, video_ids: iter):
        """
        Score specific videos
        :param video_ids: list | tuple -> Youtube video id strings
        :return: BrandSafetyVideoAudit objects
        """
        videos = self.audit_utils.get_items(video_ids, self.video_manager)
        data = BrandSafetyVideoSerializer(videos, many=True).data
        video_audits = self.audit_videos(videos=data)
        self._index_results(video_audits, [])
        return video_audits

    def audit_remaining_videos(self):
        query = QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get() \
            & QueryBuilder().build().must().exists().field(Sections.GENERAL_DATA).get()
        results = self.video_manager.search(query, limit=5000).execute().hits
        while results:
            data = BrandSafetyVideoSerializer(results, many=True).data
            video_audits = self.audit_videos(videos=data)
            self._index_results(video_audits, [])
            logger.error("Indexed {} videos".format(len(video_audits)))
            results = self.video_manager.search(query, limit=5000).execute().hits

    def _set_blacklist_data(self, items, blacklist_type=0):
        """
        Mutates each item by adding BlacklistItem data
        :param item_ids: list - > Channel or Video ids
        :param blacklist_type: 0 = Video, 1 = Channel
        :return:
        """
        item_ids = [item["id"] for item in items]
        blacklist_items = BlacklistItem.get(item_ids, blacklist_type, to_dict=True)
        for item in items:
            item[BLACKLIST_DATA] = blacklist_items.get(item["id"], None)
