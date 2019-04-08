import logging

from brand_safety import constants
from brand_safety.audit_providers.base import AuditProvider
from utils.data_providers.sdb_data_provider import SDBDataProvider
from brand_safety.audit_services.standard_brand_safety_service import StandardBrandSafetyService
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedVideo

logger = logging.getLogger("slack_update")


class StandardBrandSafetyProvider(AuditProvider):
    """
    Interface for reading source data and providing it to services
    """
    # channel_id_batch_limit = 40
    # TESTING
    channel_id_batch_limit = 10

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
        self.audit_service = StandardBrandSafetyService(self.audits, self.score_mapping)
        self.whitelist_channels = PersistentSegmentChannel.objects.get("Brand Safety Whitelist Channels")
        self.blacklist_channels = PersistentSegmentChannel.objects.get("Brand Safety Blacklist Channels")
        self.whitelist_videos = PersistentSegmentVideo.objects.get("Brand Safety Whitelist Videos")
        self.blacklist_videos = PersistentSegmentChannel.objects.get("Brand Safety Blacklist Videos")

    def run(self):
        """
        Drives main audit logic
        :return: None
        """
        for channel_batch in self.channel_id_batch_generator(self.cursor):
            video_audits = self.audit_service.audit_videos(channel_ids=channel_batch)
            sorted_video_audits = self.audit_service.sort_video_audits(video_audits)
            channel_audits = self.audit_service.audit_channels(sorted_video_audits)
            self.process_results(video_audits, channel_audits)

            self.cursor += len(channel_batch)
            self.script_tracker = self.update_cursor(self.script_tracker, self.cursor)
        logger.info("Standard Brand Safety Audit Complete.")

    def process_results(self, video_audits, channel_audits):
        video_index_result = self.index_brand_safety_results(
            self.audit_service.gather_brand_safety_results(video_audits),
            doc_type=constants.VIDEO
        )
        logger.info(video_index_result)
        channel_index_result = self.index_brand_safety_results(
            self.audit_service.gather_brand_safety_results(channel_audits),
            doc_type=constants.CHANNEL
        )
        logger.info(channel_index_result)
        self._save_results(
            audits=video_audits,
            whitelist_segment=self.whitelist_videos,
            blacklist_segment=self.blacklist_videos,
            related_segment_model=PersistentSegmentRelatedVideo
        )
        self._save_results(
            audits=channel_audits,
            whitelist_segment=self.whitelist_channels,
            blacklist_segment=self.blacklist_channels,
            related_segment_model=PersistentSegmentRelatedChannel
        )

    def _save_results(self, *_, **kwargs):
        """
        Save Video and Channel audits based on their brand safety results
        :param _:
        :param kwargs:
        :return:
        """
        audits = kwargs["audits"]
        # Whitelist segment that relates to brand safety safe entities
        whitelist_segment = kwargs["whitelist_segment"]
        blacklist_segment = kwargs["blacklist_segment"]
        # Related segment model used to instantiate database brand safety objects
        related_segment_model = kwargs["related_segment_model"]

        # Sort audits by brand safety result
        brand_safety_pass, brand_safety_fail = self._sort_brand_safety(audits)
        brand_safety_pass_pks = [audit.pk for audit in brand_safety_pass]
        brand_safety_fail_pks = [audit.pk for audit in brand_safety_fail]
        # Remove brand safety failed audits from whitelist as they are no longer belong in the whitelist
        related_segment_model.objects.filter(related_id__in=brand_safety_pass_pks).delete()
        related_segment_model.objects.filter(related_id__in=brand_safety_fail_pks).delete()

        to_create = []
        for index, audit in enumerate(brand_safety_pass + brand_safety_fail):
            segment = whitelist_segment if index < len(brand_safety_pass) else blacklist_segment
            try:
                # Change existing object segments if necessary
                obj = related_segment_model.objects.get(related_id=audit.pk)
                obj.segment = segment
                obj.save()
            except related_segment_model.DoesNotExist:
                to_create.append(
                    audit.instantiate_related_model(whitelist_segment)
                )
        related_segment_model.objects.bulk_create(to_create)

    def _sort_brand_safety(self, audits):
        brand_safety_pass = []
        brand_safety_fail = []
        for audit in audits:
            if audit.brand_safety_pass:
                brand_safety_pass.append(audit)
            else:
                brand_safety_fail.append(audit)
        return brand_safety_pass, brand_safety_fail

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
