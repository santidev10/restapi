import logging
from collections import defaultdict
from collections import namedtuple
import re
import multiprocessing as mp

from flashtext import KeywordProcessor

from brand_safety.models import BadWord
from brand_safety import constants
from brand_safety.audit_providers.base import AuditProvider
from utils.data_providers.sdb_data_provider import SDBDataProvider
from brand_safety.audit_services.standard_brand_safety_service import StandardBrandSafetyService
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedVideo

logger = logging.getLogger(__name__)


class StandardBrandSafetyProvider(AuditProvider):
    """
    Interface for reading source data and providing it to services
    """
    # channel_id_batch_limit = 40
    # TESTING
    channel_id_master_batch_limit = 50
    channel_id_pool_batch_limit = 10
    max_process_count = 5
    BrandSafetyResults = namedtuple("BrandSafetyResults", ["videos", "channels"])

    def __init__(self, *_, **kwargs):
        self.script_tracker = kwargs["api_tracker"]
        self.cursor = self.script_tracker.cursor
        self.sdb_data_provider = SDBDataProvider()
        self.audits = {
            # constants.BRAND_SAFETY: self.get_brand_safety_regexp()
            constants.BRAND_SAFETY: self.compile_trie_regexp(BadWord.objects.all().values_list("name", flat=True))
        }
        #### testing
        self.score_mapping = defaultdict(dict)
        for word in BadWord.objects.all():
            self.score_mapping[word.name] = {
                "category": word.category_ref_id,
                "score": 1, # TESTING
                # "score": word.negative_score
            }
        self.audit_service = StandardBrandSafetyService(self.audits, self.score_mapping)
        self.whitelist_channels, _ = PersistentSegmentChannel.objects.get_or_create(title="Brand Safety Whitelist Channels", category="whitelist")
        self.blacklist_channels, _ = PersistentSegmentChannel.objects.get_or_create(title="Brand Safety Blacklist Channels", category="blacklist")
        self.whitelist_videos, _ = PersistentSegmentVideo.objects.get_or_create(title="Brand Safety Whitelist Videos", category="whitelist")
        self.blacklist_videos, _ = PersistentSegmentVideo.objects.get_or_create(title="Brand Safety Blacklist Videos", category="blacklist")

    def run(self):
        """
        Drives main audit logic
        :return: None
        """
        count = 0
        logger.info("Starting standard audit from cursor: {}".format(self.cursor))
        pool = mp.Pool(processes=self.max_process_count)
        for channel_batch in self.channel_id_batch_generator(self.cursor):
            results = pool.map(self.process_audits, self.batch(channel_batch, self.channel_id_pool_batch_limit))
            extracted_results = self.extract_results(results)
            self.process_results(extracted_results["video_audits"], extracted_results["channel_audits"])
            # self.script_tracker = self.update_cursor(self.script_tracker, len(channel_batch))
            # self.cursor = self.script_tracker.cursor
            # logger.info("Last cursor position: {}".format(self.cursor))
            if self.cursor % 100000 == 0:
                logger.info("Standard Brand Safety Cursor at: {}".format(self.cursor))
            count += 1
            if count > 5:
                break
        logger.info("Standard Brand Safety Audit Complete.")

    def process_audits(self, channel_ids):
        video_audits = self.audit_service.audit_videos(channel_ids=channel_ids)
        sorted_video_audits = self.audit_service.sort_video_audits(video_audits)
        channel_audits = self.audit_service.audit_channels(sorted_video_audits)
        results = {
            "video_audits": video_audits,
            "channel_audits": channel_audits
        }
        return results

    def compile_trie_regexp(self, words):
        keyword_processor = KeywordProcessor()
        for word in words:
            keyword_processor.add_keyword(word)
        return keyword_processor

    def extract_results(self, results):
        video_audits = []
        channel_audits = []
        for batch in results:
            video_audits.extend(batch["video_audits"])
            channel_audits.extend(batch["channel_audits"])
        extracted = {
            "video_audits": video_audits,
            "channel_audits": channel_audits
        }
        return extracted

    def process_results(self, video_audits, channel_audits):
        # video_index_result = self.index_brand_safety_results(
        #     self.audit_service.gather_video_brand_safety_results(video_audits),
        #     doc_type=constants.VIDEO
        # )
        # logger.info(video_index_result)
        # channel_index_result = self.index_brand_safety_results(
        #     self.audit_service.gather_channel_brand_safety_results(channel_audits),
        #     doc_type=constants.CHANNEL
        # )
        # logger.info(channel_index_result)
        logger.info("Saving results.")
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
        # Whitelist segment that relates to brand safety safe objects
        whitelist_segment = kwargs["whitelist_segment"]
        blacklist_segment = kwargs["blacklist_segment"]
        # Related segment model used to instantiate database brand safety objects
        related_segment_model = kwargs["related_segment_model"]
        # Sort audits by brand safety result
        brand_safety_pass, brand_safety_fail = self._sort_brand_safety(audits)
        brand_safety_pass_pks = list(brand_safety_pass.keys())
        brand_safety_fail_pks = list(brand_safety_fail.keys())
        # Remove brand safety failed audits from whitelist as they are no longer belong in the whitelist
        whitelist_segment.related.all().filter(related_id__in=brand_safety_fail_pks).delete()
        blacklist_segment.related.all().filter(related_id__in=brand_safety_pass_pks).delete()
        # Get existing ids to find audits to create
        exists = related_segment_model.objects\
            .filter(related_id__in=brand_safety_pass_pks + brand_safety_fail_pks)\
            .values_list("related_id", flat=True)
        to_create = set(brand_safety_pass_pks + brand_safety_fail_pks) - set(exists)
        # Instantiate related models with appropriate segment and segment types
        to_create = [
            brand_safety_pass[pk].instantiate_related_model(related_segment_model, whitelist_segment, segment_type=constants.WHITELIST)
            if brand_safety_pass.get(pk) is not None
            else
            brand_safety_fail[pk].instantiate_related_model(related_segment_model, blacklist_segment, segment_type=constants.BLACKLIST)
            for pk in to_create
        ]
        related_segment_model.objects.bulk_create(to_create)

    def _sort_brand_safety(self, audits):
        brand_safety_pass = {}
        brand_safety_fail = {}
        for audit in audits:
            if audit.brand_safety_score["overall_score"] < 3:
                brand_safety_pass[audit.pk] = audit
            else:
                brand_safety_fail[audit.pk] = audit
        return brand_safety_pass, brand_safety_fail

    # def index_brand_safety_results(self, results, doc_type=constants.VIDEO):
    #     """
    #     Send audit results for Elastic search indexing
    #     :param results: Audit brand safety results
    #     :param doc_type: Index document type
    #     :return: Singledb response
    #     """
    #     response = self.sdb_data_provider.es_index_brand_safety_results(results, doc_type)
    #     return response

    def channel_id_batch_generator(self, cursor):
        """
        Yields batch channel ids to audit
        :param cursor: Cursor position to start audit
        :return: list -> Youtube channel ids
        """
        channel_ids = PersistentSegmentRelatedChannel.objects.all().distinct("related_id").values_list("related_id", flat=True)[cursor:]
        for batch in self.batch(channel_ids, self.channel_id_master_batch_limit):
            yield batch
