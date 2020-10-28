from .base_auditor import BaseAuditor
from brand_safety.auditors.serializers import BrandSafetyVideo
from brand_safety.audit_models.brand_safety_video_audit import BrandSafetyVideoAudit
from brand_safety.constants import BRAND_SAFETY_SCORE
from es_components.models import Channel
from es_components.models import Video


class VideoAuditor(BaseAuditor):
    es_model = Video
    VIDEO_BATCH_SIZE = 500
    VIDEO_CHANNEL_RESCORE_THRESHOLD = 60

    def __init__(self, *args, ignore_vetted_brand_safety=False, **kwargs):
        """
        Class to handle video brand safety scoring logic
        :param ignore_vetted_videos: bool -> Determines if vetted videos should be indexed after
        :param ignore_vetted_brand_safety: bool -> Determines if the script should use vetted brand safety categories
            to set category scores and overall score to 0 if not safe or to 100 if safe
            A video is determined safe or not safe by the presence of task_us_data.brand_safety categories
        :param audit_utils: AuditUtils -> Optional passing of an AuditUtils object, as it is expensive to instantiate
            since it compiles keyword processors of every brand safety BadWord row
        """
        super().__init__(*args, **kwargs)
        self._config = dict(
            ignore_vetted_brand_safety=ignore_vetted_brand_safety,
        )
        self._channels_to_rescore = []

    @property
    def channels_to_rescore(self):
        """
        Access which channels should be rescored because of a video that has scored badly
        This should be accessed after the _check_rescore_channels method has been invoked
        """
        return self._channels_to_rescore

    def audit_serialized(self, video_dict: dict) -> BrandSafetyVideoAudit:
        """
        Audit single video with serialized data
        :param video_dict:
        :return:
        """
        video = Video(video_dict["id"])
        video.populate_general_data(
            title=video_dict.get("title"),
            description=video_dict.get("description"),
            tags=video_dict.get("tags"),
        )
        # Attributes added to video instance required for audit
        with_data = BrandSafetyVideo(video).to_representation(video)
        audit = BrandSafetyVideoAudit(with_data, self.audit_utils)
        audit.run()
        return audit

    def audit_video(self, video: Video) -> BrandSafetyVideoAudit:
        """
        Audit single Video
        Video should be an instance of BrandSafetyVideo as it adds additional attributes required for audit
        :param video: Video object
        :return:
        """
        if isinstance(video, str):
            video = self.get_data([video])
        audit = BrandSafetyVideoAudit(video, self.audit_utils,
                                      ignore_vetted_brand_safety=self._config.get("ignore_vetted_brand_safety"))
        audit.run()
        return audit

    def get_data(self, video_ids: list, channel_mapping=None) -> tuple:
        """
        Retrieve Videos by id
        This also adds metadata to each Video document through BrandSafetyVideo
        :param video_ids: list
        :param channel_mapping: dict -> dict of channel id to channel
        :return: tuple
        """
        videos = self.video_manager.get(video_ids, skip_none=True)
        channel_mapping = channel_mapping or self._get_channel_mapping({v.channel.id for v in videos if v.channel.id is not None})
        with_data = BrandSafetyVideo(videos, many=True, context=dict(channels=channel_mapping)).data
        return with_data, channel_mapping

    def process_for_channel(self, channel: Channel, videos: list, index=True) -> list:
        """
        Method to handle video scoring for a single channel
        videos arg should always be videos in the channel
        :param channel: Channel
        :param videos: list [Video, ...] -> List of Video instances that are in the channel
        :param index: bool -> Determines whether to index video audit results or not
        """
        all_audits = []
        channel_mapping = {"channels": {channel.main.id: channel}}
        for batch in self.audit_utils.batch(videos, self.VIDEO_BATCH_SIZE):
            with_data = BrandSafetyVideo(batch, many=True, context=channel_mapping).data
            video_audits = [self.audit_video(video) for video in with_data]
            all_audits.extend(video_audits)
            if index:
                self.index_audit_results(self.video_manager, video_audits)
        return all_audits

    def process(self, video_ids: list, index=True, channel_mapping=None) -> None:
        """
        Audit videos ids with indexing
        :param video_ids: list[str]
        :param index: Should index results
        :param channel_mapping: dict -> dict of channel id to channel
        :return: None
        """
        for batch in self.audit_utils.batch(video_ids, self.VIDEO_BATCH_SIZE):
            videos, channel_mapping = self.get_data(batch, channel_mapping)
            video_audits = [self.audit_video(video) for video in videos]
            # For videos with low scores, check if their channel should be rescored
            check_rescore_channels = [
                channel_mapping.get(audit.doc.channel.id) for audit in video_audits
                if getattr(audit, BRAND_SAFETY_SCORE).overall_score < self.VIDEO_CHANNEL_RESCORE_THRESHOLD
            ]
            self._check_rescore_channels(check_rescore_channels)
            if index is True:
                to_index = [audit.instantiate_es() for audit in video_audits]
                self.index_audit_results(self.video_manager, to_index, size=250)

    def _get_channel_mapping(self, channel_ids: set) -> dict:
        """
        Get mapping of channel id to channel for video audits
        :param channel_ids: list [str, ...]
        :return: dict
        """
        channels = self.channel_manager.get(channel_ids, skip_none=True)
        channel_map = {
            channel.main.id: channel
            for channel in channels
        }
        return channel_map

    def _check_rescore_channels(self, channels: list) -> None:
        """
        Checks whether a new video's non vetted channel should be rescored
        If the video has a negative score, then it may have a large impact on its channels score
        Add channels to rescore to self.channels_to_rescore
        :param channels: list [Channel, ...]
        :return: None
        """
        for channel in channels:
            if not channel:
                continue
            channel_overall_score = channel.brand_safety.overall_score
            blocklisted = channel.custom_properties.blocklist is True
            if channel_overall_score and channel_overall_score > 0 and blocklisted is False:
                self._channels_to_rescore.append(channel.main.id)
