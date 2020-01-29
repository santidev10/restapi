from concurrent.futures import ThreadPoolExecutor
import itertools
import logging

from audit_tool.models import BlacklistItem
from brand_safety.constants import BRAND_SAFETY_SCORE
from brand_safety.constants import BLACKLIST_DATA
from brand_safety.audit_models.brand_safety_channel_audit import BrandSafetyChannelAudit
from brand_safety.audit_models.brand_safety_video_audit import BrandSafetyVideoAudit
from brand_safety.auditors.serializers import BrandSafetyChannelSerializer
from brand_safety.auditors.serializers import BrandSafetyVideoSerializer
from brand_safety.auditors.utils import AuditUtils
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
    CHANNEL_BATCH_SIZE = 10
    VIDEO_BATCH_SIZE = 2000
    ES_LIMIT = 10000
    VIDEO_CHANNEL_RESCORE_THRESHOLD = 60

    MINIMUM_VIEW_COUNT = 1000
    SLEEP = 2
    MAX_CYCLE_COUNT = 20 # Number of update cycles before terminating to relieve memory
    MAX_THREAD_POOL = 10
    THREAD_BATCH_SIZE = 5
    batch_counter = 0

    CHANNEL_FIELDS = ("main.id", "general_data.title", "general_data.description", "general_data.video_tags", "brand_safety.updated_at")
    VIDEO_FIELDS = ("main.id", "general_data.title", "general_data.description", "general_data.tags",
                    "general_data.language", "channel.id", "channel.title", "captions", "custom_captions")

    def __init__(self, *_, check_rescore=False, **kwargs):
        """
        :param check_rescore: bool -> Check if a channel should be rescored
            Determined if a video's overall score falls below a threshold
        """
        self.audit_utils = AuditUtils()

        # Blacklist data for current batch being processed, set by _get_channel_batch_data
        self.blacklist_data_ref = {}
        self.channel_manager = ChannelManager(
            sections=(Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS, Sections.BRAND_SAFETY),
            upsert_sections=(Sections.BRAND_SAFETY,)
        )
        self.video_manager = VideoManager(
            sections=(Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS, Sections.CHANNEL, Sections.BRAND_SAFETY,
                      Sections.CAPTIONS, Sections.CUSTOM_CAPTIONS),
            upsert_sections=(Sections.BRAND_SAFETY, Sections.CHANNEL)
        )
        self.check_rescore = check_rescore
        self.channels_to_rescore = []

    def process_channels(self, channel_ids, index=True):
        """
        Audit channels
        :param channel_ids: list[str]
        :param index: Should index results
        :return: None
        """
        video_results = []
        channel_results = []
        for batch in self.audit_utils.batch(channel_ids, self.CHANNEL_BATCH_SIZE):
            curr_batch_channel_audits = []
            curr_batch_video_audits = []
            channels = self.channel_manager.get(batch)
            serialized = BrandSafetyChannelSerializer(channels, many=True).data
            data = self._get_channel_batch_data(serialized)
            for channel in data:
                # Ignore channels that can not be indexed without required fields
                if not channel.get("id"):
                    continue
                video_audits = self.audit_videos(videos=channel["videos"], get_blacklist_data=False)
                channel["video_audits"] = video_audits

                channel_blacklist_data = self.blacklist_data_ref.get(channel["id"], {})
                channel_audit = self.audit_channel(channel, blacklist_data=channel_blacklist_data)

                curr_batch_video_audits.extend(video_audits)
                curr_batch_channel_audits.append(channel_audit)
            video_results.extend(curr_batch_video_audits)
            channel_results.extend(curr_batch_channel_audits)
            if index:
                self._index_results(curr_batch_video_audits, curr_batch_channel_audits)
        return video_results, channel_results

    def process_videos(self, video_ids, index=True):
        """
        Audit videos
        :param video_ids: list[str]\
        :param index: Should index results
        :return:
        """
        video_results = []
        for batch in self.audit_utils.batch(video_ids, self.VIDEO_BATCH_SIZE):
            videos = self.video_manager.get(batch)
            serialized = BrandSafetyVideoSerializer(videos, many=True).data
            video_audits = self.audit_videos(videos=serialized, get_blacklist_data=True)
            video_results.extend(video_audits)
            if index:
                self._index_results(video_audits, [])
        return video_results, []

    def audit_video(self, video_data: dict, blacklist_data=None, full_audit=True) -> BrandSafetyVideoAudit:
        """
        Audit single video
        :param video_data: dict -> Data to audit
            Required keys: video_id, title
            Optional keys: description, tags, transcript
        :param blacklist_data: BlacklistItem categories
        :param full_audit: Determines if full audit object or overall score is returned
        :return:
        """
        if type(video_data) is str:
            video_data = BrandSafetyVideoSerializer(self.video_manager.get([video_data])[0]).data
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

    def audit_videos(self, channel_ids=None, videos=None, get_blacklist_data=False):
        """
        Audits videos with blacklist data
            Videos with blacklist data will set their blacklisted category scores set to zero
        :param channel_ids: list[str] -> Channel ids to audit videos for
        :param videos: list -> Video ids or dictionaries with video data to audit
        :param get_blacklist_data: bool -> Retrieve BlackListItem data
        :return: list (int | BrandSafetyVideoAudit) ->
            full_audit=False: (int) BrandSafetyVideoAudit score
            full_audit=True: BrandSafetyVideoAudit object
        """
        # Set defaults here to get access to self
        video_audits = []
        if videos and channel_ids:
            raise ValueError("You must either provide video data to audit or channels to retrieve video data for.")
        elif channel_ids:
            video_data = self._get_channel_videos_executor(channel_ids)
        elif videos and type(videos[0]) is str:
            video_data = self.video_manager.get(videos)
        else:
            video_data = videos

        if get_blacklist_data:
            video_ids = [item["id"] for item in video_data]
            self.blacklist_data_ref = {
                item.item_id: item.blacklist_category
                for item in BlacklistItem.get(video_ids, 0)
            }
        for video in video_data:
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

                if self.check_rescore:
                    self._check_rescore_channel(audit)
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
        :param blacklist_data: dict: BlacklistItem
        :param full_audit: Flag to return score or audit object
        :param rescore:
        :return:
        """
        if not rescore:
            try:
                # Retrieve existing data from Elasticsearch
                response = self.audit_utils.get_items([channel_data], self.channel_manager)[0]
                audit = response.brand_safety.overall_score
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

    def _get_channel_videos_executor(self, channel_ids: list) -> list:
        """
        Get videos for channels
        :param channel_ids: list -> channel_id str
        :return:
        """
        with ThreadPoolExecutor(max_workers=self.MAX_THREAD_POOL) as executor:
            results = executor.map(self._query_channel_videos, self.audit_utils.batch(channel_ids, self.THREAD_BATCH_SIZE))
        all_results = list(itertools.chain.from_iterable(results))
        data = BrandSafetyVideoSerializer(all_results, many=True).data
        return data

    def _query_channel_videos(self, channel_ids):
        """
        Target for channel video query thread pool
        :param channel_ids: list
        :return:
        """
        query = QueryBuilder().build().must().terms().field(VIDEO_CHANNEL_ID_FIELD).value(channel_ids).get()
        results = self.video_manager.search(query, limit=self.ES_LIMIT).source(self.VIDEO_FIELDS).execute().hits
        return results

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
        self.video_manager.upsert(videos)
        self.channel_manager.upsert(channels)

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
        videos = self._get_channel_videos_executor(list(channel_data.keys()))
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

    def _set_blacklist_data(self, items, blacklist_type=0):
        """
        Mutates each item by adding BlacklistItem data
        :param items: list - > Channel or Video
        :param blacklist_type: 0 = Video, 1 = Channel
        :return:
        """
        item_ids = [item["id"] for item in items]
        blacklist_items = BlacklistItem.get(item_ids, blacklist_type, to_dict=True)
        for item in items:
            item[BLACKLIST_DATA] = blacklist_items.get(item["id"], None)

    def _check_rescore_channel(self, video_audit):
        """
        Checks whether a new video's channel should be rescored
        If the video has a negative score, then it may have a large impact on its channels score
        Add channels to rescore to self.channels_to_rescore
        :param video_audit: BrandSafetyVideoAudit
        :return:
        """
        overall_score = getattr(video_audit, BRAND_SAFETY_SCORE).overall_score
        if overall_score < self.VIDEO_CHANNEL_RESCORE_THRESHOLD:
            try:
                channel_id = video_audit.metadata["channel_id"]
                self.channels_to_rescore.append(channel_id)
            except KeyError:
                pass
