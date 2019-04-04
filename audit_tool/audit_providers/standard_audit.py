from segment.models.persistent import PersistentSegmentRelatedChannel
from audit_tool import audit_constants as constants
from audit_tool.audit_services.standard_audit_service import StandardAuditService
from audit_tool.audit_providers.base import AuditProvider
from audit_tool.data_providers.sdb_data_provider import SDBDataProvider


class StandardAuditProvider(AuditProvider):
    channel_id_batch_limit = 40
    sdb_channel_batch_limit = 40
    sdb_video_batch_limit = 10000

    def __init__(self, *_, **kwargs):
        self.script_tracker = kwargs["api_tracker"]
        self.cursor = self.script_tracker.cursor
        self.sdb_data_provider = SDBDataProvider()
        self.audits = {
            constants.BRAND_SAFETY: self.get_brand_safety_regexp()
        }
        # testing
        self.score_mapping = {
            "terrorism": 1,
            "profanity": 1,
            "drugs": 1,
            "splc": 1,
            "racial & sexist": 1,
            "gun & violence": 1,
            "pornographic": 1,
        }
        self.audit_service = StandardAuditService(self.audits, self.score_mapping)

    def run(self):
        """
        Drives main audit logic
        :return: None
        """
        for channel_batch in self.channel_id_batch_generator(self.cursor):
            video_audits = self.audit_service.audit_videos(channel_ids=channel_batch)
            sorted_video_audits = self.audit_service.sort_video_audits(video_audits)
            channel_audits = self.audit_service.audit_channels(sorted_video_audits)
            video_index_result = self.index_brand_safety_results(
                self.audit_service.gather_brand_safety_results(video_audits),
                doc_type=constants.VIDEO
            )
            channel_index_result = self.index_brand_safety_results(
                self.audit_service.gather_brand_safety_results(channel_audits),
                doc_type=constants.CHANNEL
            )
            print(video_index_result)
            print(channel_index_result)

    def index_brand_safety_results(self, results, doc_type=constants.VIDEO):
        """
        Send audit results for Elastic search indexing
        :param results: Audit brand safety results
        :param doc_type: Index document type
        :return: Singledb response
        """
        response = self.sdb_data_provider.es_index_brand_safety_results(results, doc_type)
        return response

    def channel_id_batch_generator(self, cursor):
        """
        Yields batch channel ids to audit
        :param cursor: Cursor position to start audit
        :return: list -> Youtube channel ids
        """
        channel_ids = PersistentSegmentRelatedChannel.objects.all().distinct("related_id").values_list("related_id", flat=True)[cursor:]
        for batch in self.batch(channel_ids, self.channel_id_batch_limit):
            yield batch
