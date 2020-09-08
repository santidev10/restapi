import itertools
import logging
from concurrent.futures import ThreadPoolExecutor

from brand_safety.audit_models.brand_safety_channel_audit import BrandSafetyChannelAudit
from brand_safety.audit_models.brand_safety_video_audit import BrandSafetyVideoAudit
from brand_safety.auditors.serializers import BrandSafetyChannelSerializer
from brand_safety.auditors.serializers import BrandSafetyVideoSerializer
from brand_safety.auditors.utils import AuditUtils
from brand_safety.constants import BRAND_SAFETY_SCORE
from es_components.constants import Sections
from es_components.constants import VIDEO_CHANNEL_ID_FIELD
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder

logger = logging.getLogger(__name__)

""" BrandSafetyAudit - Used to run brand safety audit on channel and video documents
Main methods: process_channels, process_videos

Procedure process_channels:
    1. Query for channel documents
    2. Transform channel document objects to dictionaries to map all values (including missing doc keys)
    3. Retrieve channel videos documents and blacklist data, transform video documents to dictionaries
    4. Instantiate BrandSafetyChannelAudit for each channel and run audit
    5. Instantiate BrandSafetyVideoAudit for each video and run audit
    6. Index results
    
Procedure process_videos:
    1. Query for video documents
    2. Transform video document objects to dictionaries to map all values (including missing doc keys)
    3. Retrieve blacklist data
    4. Instantiate BrandSafetyVideoAudit for each video and run audit
    5. Index results
"""


class BrandSafetyAudit(object):
    """
    Interface for reading source data and providing it to services
    """
    CHANNEL_BATCH_SIZE = 1
    VIDEO_BATCH_SIZE = 2000
    ES_LIMIT = 10000
    VIDEO_CHANNEL_RESCORE_THRESHOLD = 60

    MAX_THREAD_POOL = 10
    THREAD_BATCH_SIZE = 5
    CHANNEL_SECTIONS = (Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS, Sections.BRAND_SAFETY,
                        Sections.TASK_US_DATA, Sections.CUSTOM_PROPERTIES)
    VIDEO_SECTIONS = CHANNEL_SECTIONS + (Sections.CHANNEL, Sections.CAPTIONS, Sections.CUSTOM_CAPTIONS)

    def __init__(self, *_, should_check_rescore_channels=False, ignore_vetted_channels=True, ignore_vetted_videos=True,
                 ignore_blacklist_data=False, **kwargs):
        """
        :param check_rescore: bool -> Check if a channel should be rescored
            Determined if a video's overall score falls below a threshold since it could largely impact its channel score
        """
        self.channels_to_rescore = []
        self.should_check_rescore_channels = should_check_rescore_channels
        self.ignore_vetted_channels = ignore_vetted_channels
        self.ignore_vetted_videos = ignore_vetted_videos
        self.ignore_blacklist_data = ignore_blacklist_data
        self.audit_utils = AuditUtils()

        self.channel_manager = ChannelManager(
            sections=self.CHANNEL_SECTIONS,
            upsert_sections=(Sections.BRAND_SAFETY,)
        )
        self.video_manager = VideoManager(
            sections=self.VIDEO_SECTIONS,
            upsert_sections=(Sections.BRAND_SAFETY, Sections.CHANNEL)
        )

    def process_channels(self, channel_ids: list, index=True) -> tuple:
        video_results = []
        channel_results = []
        for batch in self.audit_utils.batch(channel_ids, self.CHANNEL_BATCH_SIZE):
            batch = set(batch)
            curr_batch_channel_audits = []
            curr_batch_video_audits = []

            # Blocklisted channels do not require full audit as they will immediately get an overall_score of 0
            non_blocklist_ids, blocklist_docs = self._process_blocklist(self.channel_manager, BrandSafetyChannelAudit, batch)
            serialized = self.serialize(non_blocklist_ids, doc_type="channel")

            # Set video data on each channel
            data = self._get_channel_batch_data(serialized)
            for channel in data:
                if not channel.get("id"):
                    continue
                # Audit all videos for each channel to be used for channel score
                channel["video_audits"] = [self.audit_video(video) for video in channel["videos"]]
                channel_audit = BrandSafetyChannelAudit(channel, self.audit_utils,
                                                        ignore_blacklist_data=self.ignore_blacklist_data
                                                        )
                channel_audit.run()
                curr_batch_video_audits.extend(channel["video_audits"])
                curr_batch_channel_audits.append(channel_audit)

            video_results.extend(curr_batch_video_audits)
            channel_results.extend(curr_batch_channel_audits)
            if index:
                self._index_results(curr_batch_video_audits, curr_batch_channel_audits)
                self.channel_manager.upsert(blocklist_docs)
        return video_results, channel_results

    def process_videos(self, video_ids: list, index=True) -> list:
        """
        Audit videos ids with indexing
        :param video_ids: list[str]
        :param index: Should index results
        :return:
        """
        if not isinstance(video_ids, list):
            video_ids = [video_ids]
        video_results = []
        check_rescore_channels = []
        for batch in self.audit_utils.batch(video_ids, self.VIDEO_BATCH_SIZE):
            batch = set(batch)

            # Blocklisted videos do not require full audit as they will immediately get an overall_score of 0
            non_blocklist_ids, blocklist_docs = self._process_blocklist(self.video_manager, BrandSafetyVideoAudit, batch)

            serialized = self.serialize(non_blocklist_ids)
            for video in serialized:
                if not video.get("id") or not video.get("channel_id") or not video.get("channel_title"):
                    # Ignore videos that can not be indexed without required fields
                    continue
                audit = self.audit_video(video)
                video_results.append(audit)
                # Prepare videos that have scored low to check if their channel should be rescored
                if getattr(audit, BRAND_SAFETY_SCORE).overall_score < self.VIDEO_CHANNEL_RESCORE_THRESHOLD:
                    check_rescore_channels.append(video["channel_id"])
            if index:
                self._index_results(video_results, [])
            if self.should_check_rescore_channels:
                self._check_rescore_channels(check_rescore_channels)
        return video_results

    def audit_video(self, video_data: dict) -> BrandSafetyVideoAudit:
        """
        Audit single video
        :param video_data:
            Either video_id or BrandSafetyVideoSerializer result
        :return:
        """
        if isinstance(video_data, str):
            video_doc = self.video_manager.get([video_data], skip_none=True)[0]
            video_data = BrandSafetyVideoSerializer(video_doc).data
        audit = BrandSafetyVideoAudit(video_data, self.audit_utils, ignore_blacklist_data=self.ignore_blacklist_data)
        audit.run()
        return audit

    def _get_channel_videos_executor(self, channel_ids: list) -> list:
        """
        Get videos for channels
        :param channel_ids: list -> channel_id str
        :return:
        """
        with ThreadPoolExecutor(max_workers=self.MAX_THREAD_POOL) as executor:
            results = executor.map(self._query_channel_videos,
                                   self.audit_utils.batch(channel_ids, self.THREAD_BATCH_SIZE))
        all_results = list(itertools.chain.from_iterable(results))
        data = BrandSafetyVideoSerializer(all_results, many=True).data
        return data

    def _query_channel_videos(self, channel_ids: list) -> list:
        """
        Target for channel video query thread pool
        :param channel_ids: list
        :return:
        """
        query = QueryBuilder().build().must().terms().field(VIDEO_CHANNEL_ID_FIELD).value(channel_ids).get()
        results = self.video_manager.search(query, limit=self.ES_LIMIT).execute().hits
        return results

    def _index_results(self, video_audits: list, channel_audits: list) -> tuple:
        """
        Upsert documents with brand safety data
        Check if each document should be upserted and prepare audits for Elasticsearch upsert operation
        :param video_audits: list -> BrandSafetyVideo audits
        :param channel_audits: list -> BrandSafetyChannel audits
        :return:
        """
        videos_to_upsert = []
        # Check if vetted videos should be upserted
        for audit in video_audits:
            if self.ignore_vetted_videos is True and audit.is_vetted is True:
                continue
            videos_to_upsert.append(audit.instantiate_es())
        self.video_manager.upsert(videos_to_upsert)

        channels_to_upsert = []
        for audit in channel_audits:
            if self.ignore_vetted_channels is True and audit.is_vetted is True:
                continue
            channels_to_upsert.append(audit.instantiate_es())
        self.channel_manager.upsert(channels_to_upsert)
        return videos_to_upsert, channels_to_upsert

    def _get_channel_batch_data(self, channel_batch: list) -> list:
        """
        Adds video data to BrandSafetyChannelSerializer channels
        :param channel_batch: list of BrandSafetyChannelSerializer dicts
        :return: list
        """
        channel_data = {}

        for channel in channel_batch:
            c_id = channel["id"]
            channel["videos"] = []
            channel_data[c_id] = channel

        # Get videos for channels
        videos = self._get_channel_videos_executor(list(channel_data.keys()))
        for video in videos:
            try:
                channel_id = video["channel_id"]
                channel_data[channel_id]["videos"].append(video)
            except KeyError:
                logger.error(f"Missed video: {video}")
        return list(channel_data.values())

    def _check_rescore_channels(self, channel_ids: list) ->  None:
        """
        Checks whether a new video's channel should be rescored
        If the video has a negative score, then it may have a large impact on its channels score
        Add channels to rescore to self.channels_to_rescore
        :param video_audit: BrandSafetyVideoAudit
        :return: None
        """
        channels = self.channel_manager.get(channel_ids, skip_none=True)
        for channel in channels:
            if self.ignore_vetted_channels is True and channel.task_us_data:
                continue
            channel_overall_score = getattr(channel.brand_safety, "overall_score", None)
            if channel_overall_score and channel_overall_score > 0:
                try:
                    self.channels_to_rescore.append(channel.main.id)
                except KeyError:
                    pass

    def serialize(self, ids: iter, doc_type="video") -> list:
        """
        Serialize video or channel ids
        :param ids: list
        :param doc_type: str = video | channel
        :return: list
        """
        if doc_type == "video":
            serializer = BrandSafetyVideoSerializer
            manager = self.video_manager
        else:
            serializer = BrandSafetyChannelSerializer
            manager = self.channel_manager
        serialized = serializer(manager.get(ids, skip_none=True), many=True).data
        return serialized

    def _process_blocklist(self, manager, audit_model, item_ids: set) -> tuple:
        """
        Process blocklisted items
        Determine what is blocklisted by custom_properties.blocklist field and prepare blocklist documents
            for upsert
        :param manager: es_components.manager
        :param audit_model: BrandSafetyVideoAudit | BrandSafetyChannelAudit
        :param item_ids: list
        :return: tuple
        """
        if not isinstance(item_ids, set):
            item_ids = set(item_ids)
        docs = manager.get(item_ids, skip_none=True)
        blocklist_ids = set(doc.main.id for doc in docs if doc.custom_properties.blocklist is True)
        non_blocklist_ids = item_ids - blocklist_ids
        blocklist_docs = [
            audit_model.instantiate_blocklist(_id) for _id in blocklist_ids
        ]
        return non_blocklist_ids, blocklist_docs
