import audit_tool.audit_constants as constants


class AuditService(object):
    """
    Base class for various Audit Services types to inherit shared methods and attributes from to consume source data
        and drive audit logic
    """
    def __init__(self, audit_types):
        self.audit_keyword_hit_mapping = constants.AUDIT_KEYWORD_MAPPING
        self.set_audits(audit_types)
        self.audit_types = audit_types

    def set_audits(self, audit_types):
        """
        Set audits only if the regexp are valid
        :param audit_types: dict
        :return: None
        """
        for audit, regexp in audit_types.items():
            if regexp:
                setattr(self, audit, regexp)

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
