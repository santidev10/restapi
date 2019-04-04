import audit_tool.audit_constants as constants
from audit_tool.audit_services.base import AuditService
from audit_tool.audit_models.channel_audit import ChannelAudit
from audit_tool.audit_models.video_audit import VideoAudit
from audit_tool.data_providers.sdb_data_provider import SDBDataProvider


class StandardAuditService(AuditService):
    """
    Standard Audit Class to audit existing database data
    """
    video_audits_sorted = False

    def __init__(self, audit_types, score_mapping):
        super().__init__(audit_types)
        self.sdb_data_provider = SDBDataProvider()
        self.score_mapping = score_mapping

    def audit_videos(self, video_data=None, channel_ids=None):
        """
        Audits SingleDB video data
        :param video_data: list -> Video data from SDB
        :param channel_ids: list -> Channel ids
        :return: Video audit objects
        """
        if video_data and channel_ids:
            raise ValueError("You must either provide video data to audit or channel ids to retrieve video data for.")
        if channel_ids:
            video_data = self.sdb_data_provider.get_channels_videos(channel_ids)
        video_audits = []
        for video in video_data:
            audit = VideoAudit(video, self.audit_types, source=constants.SDB, score_mapping=self.score_mapping)
            audit.execute()
            video_audits.append(audit)
        self.video_audits_sorted = True
        return video_audits

    def audit_channels(self, sorted_video_audits):
        """
        Audits Channels by retrieving channel data and using sorted Video audit objects by channel id
        :param sorted_video_audits: list -> Video Audit objects
        :return: list -> Channel Audit objects
        """
        # Ensure that video audits have been sorted by channel id for aggregation
        if self.video_audits_sorted is False:
            raise StandardAuditException("You must call sort_videos_method before calling audit_channels")
        channel_ids = list(sorted_video_audits.keys())
        sorted_channel_data = self.get_sorted_channel_data(channel_ids)
        channel_audits = []
        for channel_id, video_audits in sorted_video_audits.items():
            channel_data = sorted_channel_data.get(channel_id, None)
            if not channel_data:
                continue
            channel_audit = ChannelAudit(video_audits, self.audit_types, channel_data, source=constants.SDB)
            channel_audit.execute()
            channel_audits.append(channel_audit)
        return channel_audits

    @staticmethod
    def gather_brand_safety_results(audits):
        """
        Maps audits to their brand safety scores
        :param audits: Audit objects
        :return: list -> brand safety score dictionaries
        """
        results = []
        for audit in audits:
            brand_safety_score = audit.brand_safety_score
            # Map categories field from collections.Counter instance to dictionary
            brand_safety_score["categories"] = dict(brand_safety_score["categories"])
            results.append(brand_safety_score)
        return results

    def get_sorted_channel_data(self, channel_ids):
        """
        Retrieves singledb data with given channel ids
        :param channel_ids: list -> Youtube channel ids
        :return: dict -> channel_id: channel_data
        """
        all_channel_data = self.sdb_data_provider.get_channel_data(channel_ids)
        sorted_channel_data = {
            channel["channel_id"]: channel
            for channel in all_channel_data
        }
        return sorted_channel_data


class StandardAuditException(Exception):
    pass
