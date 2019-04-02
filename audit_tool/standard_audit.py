from .audit_services import StandardAuditService
from .data_providers import SDBDataProvider
from . import audit_constants as constants
from segment.models.persistent import PersistentSegmentRelatedChannel


class StandardAuditProvider(object):
    channel_id_batch_limit = 40
    sdb_channel_batch_limit = 40
    sdb_video_batch_limit = 10000

    def __init__(self, *args, **kwargs):
        self.script_tracker = kwargs["api_tracker"]
        self.cursor = self.script_tracker.cursor
        self.sdb_data_provider = SDBDataProvider()
        self.audits = [{
            "type": constants.BRAND_SAFETY,
            "regexp": self.brand_safety_regexp,
        }]
        self.standard_audit_mapping = {
            "test": 1
        }
        self.audit_service = StandardAuditService(self.audits)

    def configure_audit(self, data, data_type="video"):
        pass

    def run(self):
        for channel_batch in self.channel_id_batch_generator(self.cursor):
            sdb_videos = self.sdb_data_provider.get_channels_videos_batch(channel_batch)
            video_audits = self.audit_service.audit_videos(sdb_videos)
            sorted_video_audits = self.audit_service.sort_video_audits(video_audits)
            channel_audits = self.audit_service.audit_channels(sorted_video_audits)

            video_brand_safety_results = self.audit_service.calculate_brand_safety_results(video_audits)

            response = self.sdb_data_provider.es_index_brand_safety_results(video_brand_safety_results)
            print(response)

    def channel_id_batch_generator(self, cursor):
        channel_ids = PersistentSegmentRelatedChannel.objects.all().distinct("related_id").values_list("related_id", flat=True)[:cursor]
        while True:
            batch = channel_ids[:self.channel_id_batch_limit]
            yield batch
            channel_ids = channel_ids[self.channel_id_batch_limit:]
            if not channel_ids:
                break


