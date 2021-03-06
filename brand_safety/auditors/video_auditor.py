from .base_auditor import BaseAuditor
from .constants import CHANNEL_SOURCE
from .constants import VIDEO_SOURCE
from brand_safety.auditors.serializers import BrandSafetyVideo
from brand_safety.audit_models.brand_safety_video_audit import BrandSafetyVideoAudit
from brand_safety.constants import BRAND_SAFETY_SCORE
from es_components.models import Channel
from es_components.models import Video


class VideoAuditor(BaseAuditor):
    es_model = Video
    VIDEO_BATCH_SIZE = 1000
    VIDEO_CHANNEL_RESCORE_THRESHOLD = 60

    def __init__(self, *args, **kwargs):
        """
        Class to handle video brand safety scoring logic
        :param audit_utils: AuditUtils -> Optional passing of an AuditUtils object, as it is expensive to instantiate
            since it compiles keyword processors of every brand safety BadWord row
        """
        super().__init__(*args, **kwargs)
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
        transcripts_mapping = self._get_transcript_mapping([video.main.id])
        context = {"transcripts": transcripts_mapping}
        with_data = BrandSafetyVideo(video, context=context).to_representation(video)
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
        audit = BrandSafetyVideoAudit(video, self.audit_utils)
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
        videos = self.video_manager.get(video_ids, skip_none=True, source=VIDEO_SOURCE)
        channel_mapping = channel_mapping or self._get_channel_mapping({v.channel.id for v in videos if v.channel.id is not None})
        transcript_mapping = self._get_transcript_mapping(video_ids=video_ids)
        with_data = BrandSafetyVideo(videos, many=True, context=dict(channels=channel_mapping,
                                                                     transcripts=transcript_mapping)).data
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
        context = {"channels": {channel.main.id: channel}}
        for batch in self.audit_utils.batch(videos, self.VIDEO_BATCH_SIZE):
            video_ids = [video.main.id for video in batch]
            transcripts_mapping = self._get_transcript_mapping(video_ids=video_ids)
            context["transcripts"] = transcripts_mapping
            with_data = BrandSafetyVideo(batch, many=True, context=context).data
            video_audits = [self.audit_video(video) for video in with_data]
            all_audits.extend(video_audits)
            if index:
                to_index = [audit.add_brand_safety_data() for audit in video_audits]
                self.index_audit_results(self.video_manager, to_index)
        return all_audits

    def process(self, video_ids: list, index=True, channel_mapping=None, as_rescore=False) -> list:
        """
        Audit videos ids with indexing
        :param video_ids: list[str]
        :param index: Should index results
        :param channel_mapping: dict -> dict of channel id to channel
        :param as_rescore: bool -> If the batch of video_ids are being scored with a value of brand_safety.rescore=True.
            If as_rescore is True, then the scored videos will be set with rescore=False as scoring is completed
        :return: None
        """
        scored = []
        for batch in self.audit_utils.batch(video_ids, self.VIDEO_BATCH_SIZE):
            videos, channel_mapping = self.get_data(batch, channel_mapping)
            video_audits = [self.audit_video(video) for video in videos]
            # For videos with low scores, check if their channel should be rescored
            check_rescore_channels = [
                channel_mapping.get(audit.doc.channel.id) for audit in video_audits
                if getattr(audit, BRAND_SAFETY_SCORE).overall_score < self.VIDEO_CHANNEL_RESCORE_THRESHOLD
            ]
            self._check_rescore_channels(check_rescore_channels)
            scored.extend(video_audits)
            if index is True:
                to_index = [audit.add_brand_safety_data(reset_rescore=as_rescore) for audit in video_audits]
                self.index_audit_results(self.video_manager, to_index)
        return scored

    def _get_channel_mapping(self, channel_ids: set) -> dict:
        """
        Get mapping of channel id to channel for video audits
        :param channel_ids: list [str, ...]
        :return: dict
        """
        channels = self.channel_manager.get(channel_ids, skip_none=True, source=CHANNEL_SOURCE)
        channel_map = {
            channel.main.id: channel
            for channel in channels
        }
        return channel_map

    def _get_transcript_mapping(self, video_ids: list) -> dict:
        """
        get a mapping of video transcripts by video id for video audits
        :param video_ids: [str, ...]
        :return: dict
        """
        transcripts = self.transcripts_manager.get_by_video_ids(video_ids=video_ids)
        return self.audit_utils.map_transcripts_by_video_id(transcripts=transcripts)

    def _check_rescore_channels(self, channels: list) -> None:
        """
        Checks whether a new video channel should be rescored
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
