from segment.models.persistent import PersistentSegmentRelatedChannel
from .data_providers import SDBDataProvider
from audit_tool import audit_constants as constants


class StandardAudit(object):
    def __init__(self, *args, **kwargs):
        sdb_connector = kwargs['sdb_connector']
        self.VideoAudit = kwargs['VideoAudit']
        self.ChannelAudit = kwargs['ChannelAudit']
        self.script_tracker = kwargs['api_tracker']
        self.cursor = self.script_tracker.cursor
        last_channel = PersistentSegmentRelatedChannel.objects.order_by("-updated_at")
        last_channel_id = last_channel.related_id if last_channel else None
        self.sdb_data_provider = SDBDataProvider(sdb_connector, channel_batch_limit=40, video_batch_limit=10000)
        self.channel_generator = self.sdb_data_provider.get_all_channels_batch_generator(last_id=last_channel_id)
        self.
        self.audits = [{
            'type': constants.BRAND_SAFETY,
            'regexp': self.brand_safety_regexp,
        }]

    def configure_audit(self, data, data_type='video'):
        pass

    def run(self):
        for channel_batch in self.channel_generator:
            channel_data = {
                channel['channel_id']: channel
                for channel in channel_batch
            }
            videos = self.sdb_data_provider.get_channels_videos_batch()
            video_audits = self.create_videoo_audits(videos)

    def create_videoo_audits(self):
        video_audits = [
            VideoAudit()
        ]

