import logging
import multiprocessing as mp
import sys

from django.conf import settings

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
    # Number of channels to provide to pool from _channel_generator
    channel_master_batch_size = None
    # Number of processes in pool
    max_pool_count = None
    # Multiplier for max_pool_count, increases during work off hours
    pool_multiplier = None

    WORKING_HOURS_POOL_MULTIPLIER = 1
    OFF_HOURS_POOL_MULTIPLIER = 2
    # Number of channels for each process
    CHANNEL_POOL_BATCH_SIZE = 5
    # Threshold in which a channel should be updated
    UPDATE_TIME_THRESHOLD = "now-7d/d"
    CHANNEL_BATCH_COUNTER_LIMIT = 500
    ES_LIMIT = 10000
    MINIMUM_SUBSCRIBER_COUNT = 1000
    MINIMUM_VIEW_COUNT = 1000
    SLEEP = 2
    MAX_CYCLE_COUNT = 20 # Number of update cycles before terminating to relieve memory
    batch_counter = 0

    def __init__(self, *_, **kwargs):
        self._reset_max_pool_and_batch_size()
        self.audit_utils = AuditUtils()

        # If initialized with an APIScriptTracker instance, then expected to run full brand safety
        # else main run method should not be called since it relies on an APIScriptTracker instance
        self.query_creator = self._create_discovery_query if kwargs["discovery"] else self._create_update_query

        # Blacklist data for current batch being processed, set by _get_channel_batch_data
        self.blacklist_data_ref = {}

        try:
            self.script_tracker = kwargs["api_tracker"]
            self.cursor_id = self.script_tracker.cursor_id
            self.is_manual = False
        except KeyError:
            self.is_manual = True

        self.channel_manager = ChannelManager(
            sections=(Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS, Sections.BRAND_SAFETY),
            upsert_sections=(Sections.BRAND_SAFETY,)
        )
        self.video_manager = VideoManager(
            sections=(Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS, Sections.CHANNEL, Sections.BRAND_SAFETY,
                      Sections.CAPTIONS, Sections.CUSTOM_CAPTIONS),
            upsert_sections=(Sections.BRAND_SAFETY, Sections.CHANNEL)
        )

    def _reset_max_pool_and_batch_size(self):
        """
        Reset max_pool_count depending on hour of day
        channel_master_batch_size is set accordingly to provide appropriate amount of channels for pool to process
        :return:
        """
        self.max_pool_count = 2 if "rc" in settings.HOST else 4
        self.channel_master_batch_size = self.max_pool_count * self.CHANNEL_POOL_BATCH_SIZE

    def run(self):
        """
        Pools processes to handle main audit logic and processes results
            If initialized with an APIScriptTracker instance, then expected to run full brand safety
                else main run method should not be called since it relies on an APIScriptTracker instance
        :return: None
        """
        if self.is_manual:
            raise ValueError("Provider was not initialized with an APIScriptTracker instance.")

        for channel_batch in self._channel_generator(self.cursor_id):
            # Some batches may be empty if none of the channels retrieved have full data to be audited
            # _channel_generator will stop when no items are retrieved from Elasticsearch
            if not channel_batch:
                continue

            # Dynamically set config / pool count for each batch
            self.pool_multiplier = self.WORKING_HOURS_POOL_MULTIPLIER if self.audit_utils.is_working_hours() else self.OFF_HOURS_POOL_MULTIPLIER
            self.max_pool_count = self.max_pool_count * self.pool_multiplier

            pool = mp.Pool(processes=self.max_pool_count)
            results = pool.map(self._process_audits, self.audit_utils.batch(channel_batch, self.CHANNEL_POOL_BATCH_SIZE))

            # Extract nested results from each process and index into es
            video_audits, channel_audits = self._extract_results(results)
            # Index items
            self._index_results(video_audits, channel_audits)

            if self.batch_counter > self.MAX_CYCLE_COUNT:
                sys.exit()

            if self.batch_counter % 5 == 0:
                # Update config in case it has been modified
                self.audit_utils.update_config()

            self._reset_max_pool_and_batch_size()

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
            # Ignore channels that can not be indexed without required fields
            if not channel.get("id"):
                continue
            video_audits = self.audit_videos(videos=channel["videos"], get_blacklist_data=False)
            channel["video_audits"] = video_audits

            channel_blacklist_data = self.blacklist_data_ref.get(channel["id"], {})
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
                blacklist_data = BlacklistItem.get(video_data["id"], 0)[0].categories
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

    def audit_videos(self, channels=None, videos=None, get_blacklist_data=False):
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
        if get_blacklist_data:
            video_ids = [item["id"] for item in videos]
            self.blacklist_data_ref = {
                item.item_id: item.blacklist_category
                for item in BlacklistItem.get(video_ids, 0)
            }
        for video in videos:
            try:
                video = video.to_dict()
            except AttributeError:
                pass
            if not video.get("id") or not video.get("channel_id") or not video.get("channel_title"):
                # Ignore videos that can not be indexed without required fields
                continue
            try:
                blacklist_data = self.blacklist_data_ref.get(video["id"], {})
                audit = self.audit_video(video, blacklist_data=blacklist_data)
                video_audits.append(audit)
            except KeyError as e:
                # Ignore videos without full data in accessed audit
                continue
        return video_audits

    def audit_channel(self, channel_data, blacklist_data=None, full_audit=True, rescore=True):
        """
        Audit single channel
        :param channel_data:
                if rescore:
                    dict -> Data to audit
                if not rescore:
                    str -> channel id to retrieve
            Required keys: channel_id, title
            Optional keys: description, video_tags
        :return:
        """
        if not rescore:
            # Retrieve existing data from Elasticsearch
            response = self.audit_utils.get_items([channel_data], self.channel_manager)
            try:
                audit = response[0].brand_safety.overall_score
            except (IndexError, AttributeError):
                # Channel not scored
                audit = None
        else:
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
            if not data.get("id"):
                # Ignore channels that can not be indexed without required fields
                continue
            try:
                audit = self.audit_channel(data)
                channel_audits.append(audit)
            except KeyError as e:
                # Ignore channels without full data accessed during audit
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
            query = self.query_creator(cursor_id)
            response = self.channel_manager.search(query, limit=self.channel_master_batch_size, sort=("-stats.subscribers",)).execute()
            results = [item for item in response.hits if item.main.id != cursor_id]
            if not results:
                self.audit_utils.set_cursor(self.script_tracker, None, integer=False)
                break
                
            channels = BrandSafetyChannelSerializer(results, many=True).data
            data = self._get_channel_batch_data(channels)
            self.batch_counter += 1
            yield data
            cursor_id = results[-1].main.id
            self.script_tracker = self.audit_utils.set_cursor(self.script_tracker, cursor_id, integer=False)
            self.cursor_id = self.script_tracker.cursor_id


    def _create_update_query(self, cursor_id):
        query = QueryBuilder().build().must().exists().field(MAIN_ID_FIELD).get()
        query &= QueryBuilder().build().must().range().field(MAIN_ID_FIELD).gte(cursor_id).get()
        query &= QueryBuilder().build().must().range().field("stats.observed_videos_count").gt(0).get()
        query &= QueryBuilder().build().must().range().field("stats.subscribers").gte(
            self.MINIMUM_SUBSCRIBER_COUNT).get()
        query &= QueryBuilder().build().must().range().field("brand_safety.updated_at").lte(self.UPDATE_TIME_THRESHOLD).get()
        return query

    def _create_discovery_query(self, cursor_id):
        query = QueryBuilder().build().must().exists().field(MAIN_ID_FIELD).get()
        query &= QueryBuilder().build().must().range().field(MAIN_ID_FIELD).gte(cursor_id).get()
        query &= QueryBuilder().build().must().range().field("stats.observed_videos_count").gt(0).get()
        query &= QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get()
        query &= QueryBuilder().build().must().range().field("stats.subscribers").gte(
            self.MINIMUM_SUBSCRIBER_COUNT).get()
        return query

    def _get_channel_batch_data(self, channel_batch):
        """
        Adds video data to serialized channels
        :param channel_batch: list
        :return: list
        """
        channel_ids = []
        video_ids = []
        channel_data = {}

        for channel in channel_batch:
            c_id = channel["id"]
            channel["videos"] = []
            channel_ids.append(c_id)
            channel_data[c_id] = channel

        # Get videos for channels
        videos = self._get_channel_videos(list(channel_data.keys()))
        for video in videos:
            try:
                channel_id = video["channel_id"]
                channel_data[channel_id]["videos"].append(video)
                video_ids.append(video["id"])
            except KeyError:
                logger.error(f"Missed video: {video}")

        # Set reference to blacklist items for all processes to share
        blacklist_videos = BlacklistItem.get(video_ids, 0)
        blacklist_channels = BlacklistItem.get(channel_ids, 1)

        for item in blacklist_channels + blacklist_videos:
            self.blacklist_data_ref[item.item_id] = item.blacklist_category

        return list(channel_data.values())

    def manual_channel_audit(self, channel_ids: iter):
        """
        Score specific channels and videos
        :param channel_ids: list | tuple
        :return: None
        """
        channels = self.audit_utils.get_items(channel_ids, self.channel_manager)
        serialized = BrandSafetyChannelSerializer(channels, many=True).data
        channel_data = self._get_channel_batch_data(serialized)

        if len(channel_data) > 20:
            pool = mp.Pool(processes=self.max_pool_count)
            results = pool.map(self._process_audits, self.audit_utils.batch(channel_data, self.CHANNEL_POOL_BATCH_SIZE))
        else:
            # Nest results for _extract_results method
            results = [self._process_audits(channel_data)]

        # Extract nested results from each process and index into es
        video_audits, channel_audits = self._extract_results(results)
        self._index_results(video_audits, channel_audits)
        return channel_audits

    def manual_video_audit(self, video_ids: iter, blacklist_data=None):
        """_process_audits
        Score specific videos
        :param video_ids: list | tuple -> Youtube video id strings
        :return: BrandSafetyVideoAudit objects
        """
        videos = self.audit_utils.get_items(video_ids, self.video_manager)
        data = BrandSafetyVideoSerializer(videos, many=True).data

        if blacklist_data:
            self.blacklist_data_ref = blacklist_data
            video_audits = self.audit_videos(videos=data, get_blacklist_data=False)
        else:
            video_audits = self.audit_videos(videos=data, get_blacklist_data=True)
        self._index_results(video_audits, [])
        return video_audits

    def audit_all_videos(self):
        query = QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get() \
            & QueryBuilder().build().must().exists().field(Sections.GENERAL_DATA).get() \
            & QueryBuilder().build().must().range().field("stats.views").gte(self.MINIMUM_VIEW_COUNT).get()
        results = self.video_manager.search(query, limit=5000).execute().hits
        while results:
            data = BrandSafetyVideoSerializer(results, many=True).data
            video_audits = self.audit_videos(videos=data)
            self._index_results(video_audits, [])
            logger.info("BrandSafetyAudit. Indexed {} videos".format(len(video_audits)))

            self.batch_counter += 1
            if self.batch_counter > self.MAX_CYCLE_COUNT:
                sys.exit()

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
