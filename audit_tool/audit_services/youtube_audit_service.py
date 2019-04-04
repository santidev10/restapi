import re

from audit_tool.audit_services.base import AuditService
from audit_tool.audit_models.channel_audit import ChannelAudit
from audit_tool.audit_models.video_audit import VideoAudit
from utils.data_providers.youtube_data_provider import YoutubeDataProvider


class YoutubeAuditService(AuditService):
    """
    Interface for consuming source data from providers and driving brand safety logic
    """
    def __init__(self, audit_types):
        super().__init__(audit_types)
        self.audit_types = audit_types
        self.youtube_data_provider = YoutubeDataProvider()

    def audit_videos(self, video_ids=None, channel_ids=None):
        """
        Video audit logic
            Accepts both video ids or channel ids to retrieve video data for
        :param video_ids: Youtube video ids to retrieve data for
        :param channel_ids: Youtube channel ids to retrieve videos for each channel for
        :return: Video Audit objects
        """
        video_youtube_data = self.youtube_data_provider.get_video_data(video_ids) if video_ids else \
            self.youtube_data_provider.get_channel_video_data(channel_ids)
        all_video_audits = []
        for video in video_youtube_data:
            video_audit = VideoAudit(video, self.audit_types)
            video_audit.run_custom_audit()
            all_video_audits.append(video_audit)
        return all_video_audits

    def audit_channels(self, video_audits):
        """
        Channel audit logic
        :param video_audits: Video Audit objects
        :return: Channel Audit objects
        """
        all_channel_audits = []
        sorted_channel_data = self.sort_video_audits(video_audits)
        channel_ids = list(sorted_channel_data.keys())

        # sorted_channel_data is dict of channel ids with their video audits
        channel_youtube_data = self.youtube_data_provider.get_channel_data(channel_ids)
        for channel in channel_youtube_data:
            channel_video_audits = sorted_channel_data[channel['id']]
            channel_audit = ChannelAudit(channel_video_audits, self.audit_types, channel)
            channel_audit.run_custom_audit()
            all_channel_audits.append(channel_audit)
        return all_channel_audits

    @staticmethod
    def parse_video(youtube_data, regexp):
        text = ''
        text += youtube_data['snippet'].get('title', '')
        text += youtube_data['snippet'].get('description', '')
        text += youtube_data['snippet'].get('channelTitle', '')
        found = re.search(regexp, text)
        return found


class StandardAuditException(Exception):
    pass
