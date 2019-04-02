import re
from .audits import VideoAudit
from .audits import ChannelAudit
from . import audit_constants as constants


class AuditService(object):
    def __init__(self, audit_types):
        self.audit_keyword_hit_mapping = constants.AUDIT_KEYWORD_MAPPING
        self.set_audits(audit_types)
        self.audit_types = audit_types

    def set_audits(self, audit_types):
        for audit in audit_types:
            if audit:
                setattr(self, audit['type'], audit['regexp'])

    @staticmethod
    def sort_video_audits(video_audits):
        """
        Aggregates all Video Audit objects by their channel id
        :param video_audits: Video Audit objects
        :return: Dictionary with keys as channel ids and values as lists of related Video Audit objects
        """
        channel_video_audits = {}
        for video in video_audits:
            channel_id = video.metadata['channel_id']
            channel_video_audits[channel_id] = channel_video_audits.get(channel_id, [])
            channel_video_audits[channel_id].append(video)
        return channel_video_audits


class StandardAuditService(AuditService):
    def __init__(self, audit_types):
        super().__init__(audit_types)

    def audit_videos(self, video_objs):
        video_audits = []
        for video in video_objs:
            audit = VideoAudit(video, self.audit_types, source=constants.SDB)
            audit.execute()
            video_audits.append(audit)
        return video_audits

    def audit_channels(self, sorted_video_audits):
        channel_audits = []
        for video_audits in sorted_video_audits.values():
            channel_audit = ChannelAudit(video_audits, self.audit_types, source=constants.SDB)
            channel_audit.execute()
            channel_audits.append(channel_audit)
        return channel_audits


class YoutubeAuditService(AuditService):
    def __init__(self, audit_types, youtube, sdb):
        super().__init__(audit_types)
        self.youtube_data_provider = youtube()
        self.sdb_connector = sdb()

    def connector_get_channel_videos(self, channel_ids: list, fields: str) -> list:
        """
        Retrieves all videos associated with channel_ids from Singledb
        :param channel_ids: Channel id strings
        :param fields: Video fields to retrieve
        :return: video objects from Singledb
        """
        params = dict(
            fields=fields,
            sort="video_id",
            size=10000,
            channel_id__terms=",".join(channel_ids),
        )
        response = self.sdb_connector.execute_get_call("videos/", params)
        return response.get('items')

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
            channel_audit = ChannelAudit(channel_video_audits, self.audits, channel)
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
