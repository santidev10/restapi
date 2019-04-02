from .audit_services import StandardAuditService
from .data_providers import SDBDataProvider
from . import audit_constants as constants
from segment.models.persistent import PersistentSegmentRelatedChannel

class StandardAuditProvider(object):
    sdb_channel_batch_limit = 40
    sdb_video_batch_limit = 10000

    def __init__(self, *args, **kwargs):
        self.script_tracker = kwargs['api_tracker']
        self.cursor = self.script_tracker.cursor
        last_channel = PersistentSegmentRelatedChannel.objects.order_by("-related_id")
        last_channel_id = last_channel.related_id if last_channel else None
        self.sdb_data_provider = SDBDataProvider(channel_batch_limit=self.sdb_channel_batch_limit, video_batch_limit=self.sdb_video_batch_limit)
        self.channel_generator = self.sdb_data_provider.get_all_channels_batch_generator(last_id=last_channel_id)
        self.audits = [{
            'type': constants.BRAND_SAFETY,
            'regexp': self.brand_safety_regexp,
        }]
        self.standard_audit_mapping = {
            "test": 1
        }
        self.audit_service = StandardAuditService(self.audits)

    def configure_audit(self, data, data_type='video'):
        pass

    def run(self):
        for channel_batch in self.channel_generator:
            channel_data = {
                channel['channel_id']: channel
                for channel in channel_batch
            }
            videos = self.sdb_data_provider.get_channels_videos_batch(list(channel_batch.keys()))
            video_audits = self.audit_service.audit_videos(videos)
            sorted_video_audits = self.audit_service.sort_video_audits(video_audits)


