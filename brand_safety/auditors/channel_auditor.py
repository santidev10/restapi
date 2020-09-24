import itertools
import logging
from concurrent.futures import ThreadPoolExecutor

from .constants import CHANNEL_SECTIONS
from .constants import VIDEO_SECTIONS
from .serializers import BrandSafetyVideoSerializer
from brand_safety.auditors.serializers import BrandSafetyChannelSerializer
from brand_safety.audit_models.brand_safety_channel_audit import BrandSafetyChannelAudit
from brand_safety.auditors.utils import AuditUtils
from es_components.constants import VIDEO_CHANNEL_ID_FIELD
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder

from .video_auditor import VideoAuditor

logger = logging.getLogger(__name__)


class ChannelAuditor:
    CHANNEL_BATCH_SIZE = 2
    MAX_THREAD_POOL = 3

    def __init__(self, ignore_vetted_channels=True, ignore_vetted_brand_safety=False, audit_utils=None):
        """
        Class to handle video brand safety scoring logic
        :param ignore_vetted_channels: bool -> Determines if vetted channels should be indexed
        :param ignore_vetted_brand_safety: bool -> Determines if the script should use vetted brand safety categories
            to set category scores and overall score to 0 if not safe or to 100 if safe
            A channel is determined safe or not safe by the presence of task_us_data.brand_safety categories
        :param audit_utils: AuditUtils -> Optional passing of an AuditUtils object, as it is expensive to instantiate
            since it compiles keyword processors of every brand safety BadWord row
        """
        self._config = dict(
            ignore_vetted_channels=ignore_vetted_channels,
            ignore_vetted_brand_safety=ignore_vetted_brand_safety,
        )
        self.audit_utils = audit_utils or AuditUtils()
        self.channel_manager = ChannelManager(
            sections=CHANNEL_SECTIONS,
            upsert_sections=(Sections.BRAND_SAFETY,)
        )
        self.video_manager = VideoManager(
            sections=VIDEO_SECTIONS,
            upsert_sections=(Sections.BRAND_SAFETY, Sections.CHANNEL)
        )
        self.video_auditor = VideoAuditor(audit_utils=self.audit_utils)

    def get_data(self, channel_ids: list) -> list:
        """
        Retrieve Channels and add data to instances using BrandSafetyChannelSerializer
        :param channel_ids: list
        :return: list
        """
        channels = self.channel_manager.get(channel_ids, skip_none=True)
        with_data = BrandSafetyChannelSerializer(channels, many=True).data
        return with_data

    def process(self, channel_ids: list, index=True):
        """
        Handle channel brand safety scoring
        This method batches channel_ids to retrieve channels, retrieve channel videos, run brand safety scoring, and
            indexes results
        :param channel_ids: list
        :param index: bool -> Determines whether to index results or not
        :return:
        """
        for batch in self.audit_utils.batch(channel_ids, self.CHANNEL_BATCH_SIZE):
            channels = self.get_data(batch)
            # Set video data on each channel
            with_videos = self._get_channel_batch_data(channels)

            channel_batch_audits = []
            video_batch_audits = []
            for channel in with_videos:
                # Audit all videos for each channel to be used for channel score
                channel.video_audits = self.video_auditor.process_for_channel(channel, channel.videos, index=index)
                channel_audit = BrandSafetyChannelAudit(channel, self.audit_utils,
                                                        ignore_vetted_brand_safety=self._config.get("ignore_vetted_brand_safety"))
                channel_audit.run()
                channel_batch_audits.append(channel_audit)
                video_batch_audits.extend(channel_audit.video_audits)
            # Index each batch as some channels have increasingly large numbers of videos
            if index:
                self.audit_utils.index_audit_results(self.channel_manager, channel_batch_audits,
                                                     self._config.get("ignore_vetted_channels"))

    def _get_channel_videos_executor(self, channel_ids: list) -> list:
        """
        Get videos for channels
        :param channel_ids: list -> channel_ids [str, ...]
        :return: list
        """
        with ThreadPoolExecutor(max_workers=self.MAX_THREAD_POOL) as executor:
            results = executor.map(self._query_channel_videos, self.audit_utils.batch(channel_ids, 1))
        all_results = list(itertools.chain.from_iterable(results))
        data = BrandSafetyVideoSerializer(all_results, many=True).data
        return data

    def _query_channel_videos(self, channel_ids: list) -> list:
        """
        Target for channel video query thread pool
        :param channel_ids: list
        :return:
        """
        query = QueryBuilder().build().must().terms().field(VIDEO_CHANNEL_ID_FIELD).value(channel_ids).get() \
            & QueryBuilder().build().must().exists().field(Sections.GENERAL_DATA).get()

        results = self.video_manager.search(query, limit=10000).execute().hits
        return results

    def _get_channel_batch_data(self, channel_batch: list) -> list:
        """
        Adds video data to BrandSafetyChannelSerializer channels for channel audit
        :param channel_batch: list of BrandSafetyChannelSerializer dicts
        :return: list
        """
        channel_data = {
            channel.main.id: channel for channel in channel_batch
        }
        # Get videos for channels
        videos = self._get_channel_videos_executor(list(channel_data.keys()))
        for video in videos:
            try:
                channel = channel_data[video.channel.id]
                channel.videos = getattr(channel_data[video.channel.id], "videos", [])
                channel.videos.append(video)
            except KeyError:
                logger.error(f"Missed video: {video}")
        return list(channel_data.values())
