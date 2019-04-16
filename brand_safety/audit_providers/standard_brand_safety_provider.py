import logging
from collections import defaultdict
import multiprocessing as mp

from django.db.models import Q

from brand_safety.models import BadWord
from brand_safety import constants
from brand_safety.audit_providers.base import AuditProvider
from brand_safety.audit_services.standard_brand_safety_service import StandardBrandSafetyService
from singledb.connector import SingleDatabaseApiConnector
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedVideo

logger = logging.getLogger(__name__)


class StandardBrandSafetyProvider(object):
    """
    Interface for reading source data and providing it to services
    """
    channel_id_master_batch_limit = 400
    channel_id_pool_batch_limit = 40
    max_process_count = 5
    brand_safety_fail_threshold = 3
    cursor_logging_threshold = 500
    # Multiplier to apply for brand safety hits
    brand_safety_score_multiplier = {
        "title": 4,
        "description": 1,
        "tags": 1,
        "transcript": 1,
    }

    def __init__(self, *_, **kwargs):
        self.script_tracker = kwargs["api_tracker"]
        self.cursor = self.script_tracker.cursor
        self.audit_provider = AuditProvider()
        self.sdb_connector = SingleDatabaseApiConnector()
        # Audit mapping for audit objects to use
        self.audits = {
            constants.BRAND_SAFETY: self.audit_provider.get_trie_keyword_processor(BadWord.objects.all().values_list("name", flat=True)),
            constants.EMOJI: self.audit_provider.compile_emoji_regexp()
        }
        # Score mapping for brand safety keywords
        self.score_mapping = self.get_brand_safety_score_mapping()
        self.audit_service = StandardBrandSafetyService(self.audits, self.score_mapping, self.brand_safety_score_multiplier)
        # Set required persistent segments to save to
        self.whitelist_channels, _ = PersistentSegmentChannel.objects.get_or_create(title="Brand Safety Whitelist Channels", category="whitelist")
        self.blacklist_channels, _ = PersistentSegmentChannel.objects.get_or_create(title="Brand Safety Blacklist Channels", category="blacklist")
        self.whitelist_videos, _ = PersistentSegmentVideo.objects.get_or_create(title="Brand Safety Whitelist Videos", category="whitelist")
        self.blacklist_videos, _ = PersistentSegmentVideo.objects.get_or_create(title="Brand Safety Blacklist Videos", category="blacklist")

    def run(self):
        """
        Pools processes to handle main audit logic and processes results
        :return: None
        """
        logger.info("Starting standard audit from cursor: {}".format(self.cursor))
        # Update brand safety scores in case they have been modified
        self.score_mapping = self.get_brand_safety_score_mapping()
        pool = mp.Pool(processes=self.max_process_count)
        for channel_batch in self.channel_id_batch_generator(self.cursor):
            results = pool.map(self.process_audits, self.audit_provider.batch(channel_batch, self.channel_id_pool_batch_limit))
            # Extract nested results from each process
            video_audits, channel_audits = self.extract_results(results)
            self.process_results(video_audits, channel_audits)
            # Update script tracker and cursors in case of failure
            self.script_tracker = self.audit_provider.update_cursor(self.script_tracker, len(channel_batch))
            self.cursor = self.script_tracker.cursor
            if self.cursor % self.cursor_logging_threshold == 0:
                logger.info("Standard Brand Safety Cursor at: {}".format(self.cursor))
        logger.info("Standard Brand Safety Audit Complete.")

    def process_audits(self, channel_ids):
        """
        Drives main brand safety logic for each process
        :param channel_ids: Channel ids to retrieve video data for
        :return:
        """
        video_audits = self.audit_service.audit_videos(channel_ids=channel_ids)
        sorted_video_audits = self.audit_service.sort_video_audits(video_audits)
        channel_audits = self.audit_service.audit_channels(sorted_video_audits)
        results = {
            "video_audits": video_audits,
            "channel_audits": channel_audits
        }
        return results

    def extract_results(self, results):
        """
        Extracts nested results from each of the processes
        :param results: list -> Dict results from each process
        :return:
        """
        video_audits = []
        channel_audits = []
        for batch in results:
            video_audits.extend(batch["video_audits"])
            channel_audits.extend(batch["channel_audits"])
        return video_audits, channel_audits

    def process_results(self, video_audits, channel_audits):
        """
        Sends request to index results in Elasticsearch and save to db segments
        :param video_audits:
        :param channel_audits:
        :return:
        """
        self.index_brand_safety_results(
            self.audit_service.gather_brand_safety_results(video_audits),
            doc_type=constants.VIDEO
        )
        self.index_brand_safety_results(
            self.audit_service.gather_brand_safety_results(channel_audits),
            doc_type=constants.CHANNEL
        )
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
            related_segment_model=PersistentSegmentRelatedChannel,
            type="channel"
        )

    def _save_results(self, *_, **kwargs):
        """
        Save Video and Channel audits based on their brand safety results to their respective persistent segments
        :param kwargs: Persistent segments to save to
        :return: None
        """
        audits = kwargs["audits"]
        # Persistent segments that store brand safety objects
        whitelist_segment = kwargs["whitelist_segment"]
        blacklist_segment = kwargs["blacklist_segment"]
        # Related segment model used to instantiate database objects
        related_segment_model = kwargs["related_segment_model"]
        # Sort audits by brand safety results
        brand_safety_pass, brand_safety_fail = self._sort_brand_safety(audits)
        brand_safety_pass_pks = list(brand_safety_pass.keys())
        brand_safety_fail_pks = list(brand_safety_fail.keys())
        # Remove brand safety failed audits from whitelist as they are no longer belong in the whitelist
        whitelist_segment.related.filter(related_id__in=brand_safety_fail_pks).delete()
        blacklist_segment.related.filter(related_id__in=brand_safety_pass_pks).delete()
        # Get existing ids to find results to create (that have been deleted from their segment based on their new result)
        exists = related_segment_model.objects\
            .filter(
                Q(segment=whitelist_segment) | Q(segment=blacklist_segment),
                related_id__in=brand_safety_pass_pks + brand_safety_fail_pks
            ).values_list("related_id", flat=True)
        # Set difference to get audits that need to be created
        to_create = set(brand_safety_pass_pks + brand_safety_fail_pks) - set(exists)
        # Instantiate related models with new appropriate segment and segment types
        to_create = [
            brand_safety_pass[pk].instantiate_related_model(related_segment_model, whitelist_segment, segment_type=constants.WHITELIST)
            if brand_safety_pass.get(pk) is not None
            else
            brand_safety_fail[pk].instantiate_related_model(related_segment_model, blacklist_segment, segment_type=constants.BLACKLIST)
            for pk in to_create
        ]
        related_segment_model.objects.bulk_create(to_create)

    def _sort_brand_safety(self, audits):
        """
        Sort audits by fail or pass based on their overall brand safety score
        :param audits: list
        :return: tuple -> lists of sorted audits
        """
        brand_safety_pass = {}
        brand_safety_fail = {}
        for audit in audits:
            if audit.brand_safety_score["overall_score"] < self.brand_safety_fail_threshold:
                brand_safety_pass[audit.pk] = audit
            else:
                brand_safety_fail[audit.pk] = audit
        return brand_safety_pass, brand_safety_fail

    def index_brand_safety_results(self, results, doc_type=constants.VIDEO):
        """
        Send audit results for Elastic search indexing
        :param results: Audit brand safety results
        :param doc_type: Index document type
        :return: Singledb response
        """
        response = self.sdb_connector.post_brand_safety_results(results, doc_type)
        return response

    def channel_id_batch_generator(self, cursor):
        """
        Yields batch channel ids to audit
        :param cursor: Cursor position to start audit
        :return: list -> Youtube channel ids
        """
        #TEST
        channel_ids = PersistentSegmentRelatedChannel.objects.all().distinct("related_id").order_by("related_id").values_list("related_id", flat=True)[cursor:]
        # channel_ids = ["UC-4OTZKFWT6_sqAaMBRnoxw", "UC-3OU7CFoaLLXUR3bWyq1Tg"]
        for batch in self.audit_provider.batch(channel_ids, self.channel_id_master_batch_limit):
            yield batch

    @staticmethod
    def get_brand_safety_score_mapping():
        """
        Map brand safety BadWord rows to their score
        :return: dict
        """
        score_mapping = defaultdict(dict)
        for word in BadWord.objects.all():
            score_mapping[word.name] = {
                "category": word.category_ref_id,
                "score": word.negative_score
            }
        return score_mapping
