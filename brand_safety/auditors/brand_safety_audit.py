from collections import Counter
from collections import defaultdict
from datetime import datetime
import logging
import multiprocessing as mp
import time

import pytz

from brand_safety.constants import BRAND_SAFETY_SCORE
from brand_safety.audit_models.brand_safety_channel_audit import BrandSafetyChannelAudit
from brand_safety.audit_models.brand_safety_video_audit import BrandSafetyVideoAudit
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
    UPDATE_TIME_THRESHOLD = 24 * 7
    CHANNEL_BATCH_COUNTER_LIMIT = 500
    ES_LIMIT = 10000
    MINIMUM_SUBSCRIBER_COUNT = 20000
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
            self._set_discovery_config()
        else:
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
        self.MAX_POOL_COUNT = 8
        self.CHANNEL_POOL_BATCH_SIZE = 20
        self.CHANNEL_MASTER_BATCH_SIZE = self.MAX_POOL_COUNT * self.CHANNEL_POOL_BATCH_SIZE
        self._channel_generator = self._channel_generator_discovery

    def _set_update_config(self):
        self.MAX_POOL_COUNT = 3
        self.CHANNEL_POOL_BATCH_SIZE = 10
        self.CHANNEL_MASTER_BATCH_SIZE = self.MAX_POOL_COUNT * self.CHANNEL_POOL_BATCH_SIZE
        self._channel_generator = self._channel_generator_update

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
            results = pool.map(self._process_audits,
                               self.audit_utils.batch(channel_batch, self.CHANNEL_POOL_BATCH_SIZE))
            # Extract nested results from each process and index into es
            video_audits, channel_audits = self._extract_results(results)
            # Index items
            self._index_results(video_audits, channel_audits)
            print(f"scored {len(channel_batch)} channels")
            print(f"scored {len(video_audits)} videos")

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
        for channel in channels:
            video_audits = self.audit_videos(videos=channel["videos"])
            channel["video_audits"] = video_audits
            channel_audit = self.audit_channel(channel)

            results["video_audits"].extend(video_audits)
            results["channel_audits"].append(channel_audit)
        return results

    def audit_video(self, video_data: dict, full_audit=True) -> BrandSafetyVideoAudit:
        """
        Audit single video
        :param video_data: dict -> Data to audit
            Required keys: video_id, title
            Optional keys: description, tags, transcript
        :return:
        """
        # Every audit should have language_processors in config
        audit = BrandSafetyVideoAudit(
            video_data,
            self.audit_utils
        )
        audit.run()
        if not full_audit:
            audit = getattr(audit, BRAND_SAFETY_SCORE).overall_score
        return audit

    def audit_videos(self, channels=None, videos=None):
        """
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
        for video in videos:
            try:
                video = video.to_dict()
            except AttributeError:
                pass
            try:
                audit = self.audit_video(video)
                video_audits.append(audit)
            except KeyError as e:
                # Ignore videos without full data
                continue
        return video_audits

    def audit_channel(self, channel_data, full_audit=True):
        """
        Audit single channel
        :param channel_data: dict -> Data to audit
            Required keys: channel_id, title
            Optional keys: description, video_tags
        :return:
        """
        audit = BrandSafetyChannelAudit(channel_data, self.audit_utils)
        audit.run()
        if not full_audit:
            audit = getattr(audit, BRAND_SAFETY_SCORE).overall_score
        return audit

    def audit_channels(self, channel_video_audits: dict = None) -> list:
        """
        Audits Channels by retrieving channel data and using sorted Video audit objects by channel id
        :param channel_video_audits: BrandSafetyVideoAudit objects
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
        mapped = []
        for batch in self.audit_utils.batch(channel_ids, 20):
            query = QueryBuilder().build().must().terms().field(VIDEO_CHANNEL_ID_FIELD).value(batch).get()
            results = self.video_manager.search(query, limit=self.ES_LIMIT).execute().hits
            all_results.extend(results)
        for video in all_results:
            try:
                mapped.append(self.audit_utils.extract_video_data(video))
            except AttributeError:
                continue
        return mapped

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

    def _channel_generator_update(self, cursor_id=None):
        """
        Yields channels to audit
        :param cursor_id: Cursor position to start audit
        :return: list -> Elasticsearch channel documents
        """
        cursor_id = cursor_id or ""
        while True:
            query = QueryBuilder().build().must().range().field(MAIN_ID_FIELD).gte(cursor_id).get()
            query &= QueryBuilder().build().must().exists().field(Sections.BRAND_SAFETY).get()
            query &= QueryBuilder().build().must().range().field("stats.subscribers").gte(self.MINIMUM_SUBSCRIBER_COUNT).get()
            response = self.channel_manager.search(query, limit=self.CHANNEL_MASTER_BATCH_SIZE, sort=("-stats.subscribers",)).execute()
            results = response.hits
            if not results:
                self.audit_utils.set_cursor(self.script_tracker, None, integer=False)
                break
            to_update = self._get_channels_to_update(results, check_last_updated=True)
            yield to_update
            cursor_id = results[-1].main.id
            self.script_tracker = self.audit_utils.set_cursor(self.script_tracker, cursor_id, integer=False)
            self.cursor_id = self.script_tracker.cursor_id
            self.channel_batch_counter += 1

    def _channel_generator_discovery(self, cursor_id=None):
        """
        Get channels to score with no brand safety data
        :param cursor_id:
        :return:
        """
        cursor_id = cursor_id or ""
        while True:
            query = QueryBuilder().build().must().range().field(MAIN_ID_FIELD).gte(cursor_id).get()
            query &= QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get()
            query &= QueryBuilder().build().must().range().field("stats.subscribers").gte(self.MINIMUM_SUBSCRIBER_COUNT).get()
            response = self.channel_manager.search(query, limit=self.CHANNEL_MASTER_BATCH_SIZE, sort=("-stats.subscribers",)).execute()
            results = response.hits
            if not results:
                self.audit_utils.set_cursor(self.script_tracker, None, integer=False)
                break
            to_score = self._get_channels_to_update(results, check_last_updated=False)
            yield to_score
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
        channels = {}
        for item in channel_batch:
            try:
                channels[item.main.id] = self.audit_utils.extract_channel_data(item)
            except AttributeError:
                continue

        channels_to_update = []
        # Get videos for channels
        videos = self._get_channel_videos(list(channels.keys()))
        videos_by_channel = defaultdict(list)
        for video in videos:
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
            try:
                mapped = self.audit_utils.extract_channel_data(item["_source"])
                mapped["videos"] = self._get_channel_videos([mapped["id"]])
                to_audit.append(mapped)
            except KeyError:
                continue
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
        mapped = []
        for item in videos:
            try:
                mapped.append(self.audit_utils.extract_video_data(item["_source"]))
            except KeyError:
                continue
        video_audits = self.audit_videos(videos=mapped)
        self._index_results(video_audits, [])
        return video_audits
