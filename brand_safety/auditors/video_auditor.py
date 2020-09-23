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
from es_components.models import Video
from es_components.models import Channel

from es_components.constants import Sections
from es_components.managers import VideoManager
from brand_safety.auditors.utils import AuditUtils
from brand_safety.auditors.serializers import BrandSafetyVideoSerializer


class VideoAuditor:
    VIDEO_BATCH_SIZE = 2000
    VIDEO_CHANNEL_RESCORE_THRESHOLD = 60

    def __init__(self, should_check_rescore_channels=True, ignore_vetted_videos=True, ignore_blacklist_data=False,
                 audit_utils=None):
        """

        :param should_check_rescore_channels:
        :param ignore_vetted_videos:
        :param ignore_blacklist_data:
        :param audit_utils:
        """
        self._kwargs = dict(
            should_check_rescore_channels=should_check_rescore_channels,
            ignore_vetted_videos=ignore_vetted_videos,
            ignore_blacklist_data=ignore_blacklist_data,
        )
        self.channels_to_rescore = []
        self.audit_utils = audit_utils or AuditUtils()
        self.video_manager = VideoManager(
            sections=(
                Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS, Sections.BRAND_SAFETY,
                Sections.TASK_US_DATA, Sections.CUSTOM_PROPERTIES, Sections.CHANNEL, Sections.CAPTIONS,
                Sections.CUSTOM_CAPTIONS),
            upsert_sections=(Sections.BRAND_SAFETY, Sections.CHANNEL)
        )
        self.channel_manager = ChannelManager(sections=(Sections.CUSTOM_PROPERTIES, Sections.TASK_US_DATA))

    def audit_video(self, video) -> BrandSafetyVideoAudit:
        """
        Audit single video
        :param video:
        :return:
        """
        if isinstance(video, str):
            video = self.get_videos([video])
        audit = BrandSafetyVideoAudit(video, self.audit_utils,
                                      ignore_blacklist_data=self._kwargs.get("ignore_blacklist_data"))
        audit.run()
        return audit

    def get_videos(self, video_ids):
        videos = self.video_manager.get(video_ids, skip_none=True)
        with_data = BrandSafetyVideoSerializer(videos, many=True).data
        return with_data

    def process(self, video_ids: list, index=True, channel_blocklist_mapping=None) -> list:
        """
        Audit videos ids with indexing
        :param video_ids: list[str]
        :param index: Should index results
        :param channel_blocklist_mapping: dict -> dict of channel id to blocklist value
        :return:
        """
        video_results = []
        check_rescore_channels = []
        for batch in self.audit_utils.batch(video_ids, self.VIDEO_BATCH_SIZE):
            videos = self.get_videos(batch)

            to_score, blocklist_docs = self._clean_video_blocklist(videos, channel_blocklist_mapping)
            to_score, vetted_docs = self._clean_video_vetted(to_score)

            for video in to_score:
                audit = self.audit_video(video)
                video_results.append(audit)
                # Prepare videos that have scored low to check if their channel should be rescored
                if getattr(audit, BRAND_SAFETY_SCORE).overall_score < self.VIDEO_CHANNEL_RESCORE_THRESHOLD:
                    check_rescore_channels.append(video.channel.id)
            if index is True:
                self.index_results(video_results)
                self.video_manager.upsert(blocklist_docs + vetted_docs)

            if self._kwargs.get("should_check_rescore_channels"):
                self._check_rescore_channels(check_rescore_channels)
        return video_results

    def get_to_score(self, videos, channel_blocklist_mapping):
        to_score, blocklist_docs = self._clean_video_blocklist(videos, channel_blocklist_mapping)
        to_score, vetted_docs = self._clean_video_vetted(to_score)
        return to_score, vetted_docs

    def _clean_video_blocklist(self, videos: list, channel_blocklist_mapping: dict):
        """
        Method to separate blocklisted videos from videos to score as blocklist videos should not go through
        normal flow
        Blocklist video scores are automatically set with overall score = 0
        :param videos: list -> self.get_videos result
        :param channel_blocklist_mapping: dict -> Mapping of channel id to channel's blocklist boolean value
        :return:
        """
        # If channel is blocklisted videos are implicitly blocklisted
        if not channel_blocklist_mapping:
            batch_channel_blocklist = self._get_channel_blocklist(video.channel.id for video in videos)
        else:
            batch_channel_blocklist = channel_blocklist_mapping

        # Blocklisted channels do not require full audit as they will immediately get an overall_score of 0
        non_blocklist = []
        blocklist_docs = []
        for video in videos:
            if video.custom_properties.blocklist is True or batch_channel_blocklist.get(video.channel.id) is True:
                blocklist_docs.append(BrandSafetyVideoAudit.instantiate_blocklist(video.main.id))
            else:
                non_blocklist.append(video)
        return non_blocklist, blocklist_docs

    def _clean_video_vetted(self, videos: list) -> tuple:
        """
        Separate vetted documents as they do not need to be fully vetted
        If has task_us_data.brand_safety categories, set all scores to 0
        Else set all scores to 100
        :param serialized_videos: list -> self.serialize result
        :return: tuple
        """
        to_score = []
        vetted_to_upsert = []
        for video in videos:
            if video.task_us_data.last_vetted_at is not None:
                video = Video(video.init_main.id)
                if any(category for category in video.task_us_data.brand_safety):
                    bs_data = dict(overall_score=0, **self.audit_utils.default_zero_score)
                else:
                    bs_data = dict(overall_score=100, **self.audit_utils.default_full_score)
                video.populate_brand_safety(**bs_data)
                vetted_to_upsert.append(video)
            else:
                to_score.append(video)
        return to_score, vetted_to_upsert

    def _get_channel_blocklist(self, channel_ids):
        channels = self.channel_manager.get([_id for _id in channel_ids if _id is not None], skip_none=True)
        blocklist_map = {
            channel.main.id: channel.custom_properties.blocklist
            for channel in channels
        }
        return blocklist_map

    def index_results(self, video_audits: list):
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
            if self._kwargs.get("ignore_vetted_videos") is True and audit.video.task_us_data.last_vetted_at is not None:
                continue
            videos_to_upsert.append(audit.instantiate_es())
        self.video_manager.upsert(videos_to_upsert, refresh=False)
        return videos_to_upsert

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
            if self._kwargs.get("ignore_vetted_channels") is True and channel.task_us_data:
                continue
            channel_overall_score = channel.brand_safety.overall_score
            blocklisted = channel.custom_properties.blocklist is True
            if channel_overall_score and channel_overall_score > 0 and blocklisted is False:
                try:
                    self.channels_to_rescore.append(channel.main.id)
                except (KeyError, TypeError):
                    pass
