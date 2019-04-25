import logging
from collections import defaultdict
import multiprocessing as mp

from utils.elasticsearch import ElasticSearchConnector
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from brand_safety import constants
from brand_safety.audit_providers.base import AuditProvider
from brand_safety.audit_services.standard_brand_safety_service import StandardBrandSafetyService
from singledb.connector import SingleDatabaseApiConnector

logger = logging.getLogger(__name__)


class StandardBrandSafetyProvider(object):
    """
    Interface for reading source data and providing it to services
    """
    channel_id_master_batch_limit = 500
    channel_id_pool_batch_limit = 50
    max_process_count = 10
    cursor_id_logging_threshold = 50000
    brand_safety_fail_threshold = 3
    # Multiplier to apply for brand safety hits
    brand_safety_score_multiplier = {
        "title": 4,
        "description": 1,
        "tags": 1,
        "transcript": 1,
    }
    # Bad words in these categories should be ignored while calculating brand safety scores
    bad_word_categories_ignore = [3]
    channel_batch_counter = 0
    channel_batch_counter_limit = 500

    def __init__(self, *_, **kwargs):
        self.script_tracker = kwargs["api_tracker"]
        self.cursor_id = self.script_tracker.cursor
        self.audit_provider = AuditProvider()
        self.sdb_connector = SingleDatabaseApiConnector()
        # Audit mapping for audit objects to use
        self.audits = {
            constants.BRAND_SAFETY: self.audit_provider.get_trie_keyword_processor(self.get_bad_words()),
            constants.EMOJI: self.audit_provider.compile_emoji_regexp()
        }
        # Initial category brand safety scores for videos and channels, since ignoring certain categories (e.g. Kid's Content)
        self.default_video_category_scores = self.create_brand_safety_default_category_scores(data_type=constants.VIDEO)
        self.default_channel_category_scores = self.create_brand_safety_default_category_scores(data_type=constants.CHANNEL)
        self.audit_service = StandardBrandSafetyService(
            audit_types=self.audits,
            score_mapping=self.get_brand_safety_score_mapping(),
            score_multiplier=self.brand_safety_score_multiplier,
            default_video_category_scores=self.default_video_category_scores,
            default_channel_category_scores=self.default_channel_category_scores,
            es_video_index=constants.BRAND_SAFETY_VIDEO_ES_INDEX,
            es_channel_index=constants.BRAND_SAFETY_CHANNEL_ES_INDEX
        )
        self.es_connector = ElasticSearchConnector()

    def run(self):
        """
        Pools processes to handle main audit logic and processes results
        :return: None
        """
        logger.info("Starting standard audit...")
        pool = mp.Pool(processes=self.max_process_count)
        for channel_batch in self.channel_id_batch_generator(self.cursor_id):
            # Update score mapping so each batch uses updated brand safety scores
            results = pool.map(self._process_audits, self.audit_provider.batch(channel_batch, self.channel_id_pool_batch_limit))
            # Extract nested results from each process
            video_audits, channel_audits = self._extract_results(results)
            self._process_results(video_audits, channel_audits)
            # Update script tracker and cursors in case of failure
            self.script_tracker = self.audit_provider.set_cursor(self.script_tracker, channel_batch[-1])
            self.cursor_id = self.script_tracker.cursor
            # Update brand safety scores in case they have been modified
            self.audit_service.score_mapping = self.get_brand_safety_score_mapping()
        logger.info("Standard Brand Safety Audit Complete.")
        self.audit_provider.set_cursor(self.script_tracker, "0", integer=False)

    def _process_audits(self, channel_ids):
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

    def _extract_results(self, results):
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

    def _process_results(self, video_audits, channel_audits):
        """
        Sends request to index results in Elasticsearch and save to db segments
        :param video_audits:
        :param channel_audits:
        :return:
        """
        self.index_brand_safety_results(
            # self.audit_service.gather_brand_safety_results(video_audits),
            video_audits,
            index_name=constants.BRAND_SAFETY_VIDEO_ES_INDEX
        )
        self.index_brand_safety_results(
            # self.audit_service.gather_brand_safety_results(channel_audits),
            channel_audits,
            index_name=constants.BRAND_SAFETY_CHANNEL_ES_INDEX
        )

    def _sort_brand_safety(self, audits):
        """
        Sort audits by fail or pass based on their overall brand safety score
        :param audits: list
        :return: tuple -> lists of sorted audits
        """
        brand_safety_pass = {}
        brand_safety_fail = {}
        for audit in audits:
            if audit.brand_safety_score.overall_score < self.brand_safety_fail_threshold:
                brand_safety_pass[audit.pk] = audit
            else:
                brand_safety_fail[audit.pk] = audit
        return brand_safety_pass, brand_safety_fail

    def index_brand_safety_results(self, results, index_name):
        index_type = "score"
        op_type = "index"
        es_bulk_generator = (audit.es_repr(index_name, index_type, op_type) for audit in results)
        self.es_connector.push_to_index(es_bulk_generator)

    def channel_id_batch_generator(self, cursor_id):
        """
        Yields batch channel ids to audit
        :param cursor_id: Cursor position to start audit
        :return: list -> Youtube channel ids
        """
        cursor_id = cursor_id if cursor_id is not "0" else None
        params = {
            "fields": "channel_id",
            "sort": "channel_id",
            "size": self.channel_id_master_batch_limit,
        }
        while self.channel_batch_counter <= self.channel_batch_counter_limit:
            params["channel_id__range"] = "{},".format(cursor_id)
            response = self.sdb_connector.get_channel_list(params, ignore_sources=True)
            channels = [item for item in response.get("items", []) if item["channel_id"] != cursor_id]
            if not channels:
                break
            yield channels
            cursor_id = channels[-1]["channel_id"]
            self.channel_batch_counter += 1

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

    def get_bad_words(self):
        """
        Get brand safety words
            Kid's content brand safety words are not included in brand safety score calculations
        :return:
        """
        bad_words = BadWord.objects\
            .exclude(category_ref_id__in=self.bad_word_categories_ignore)\
            .values_list("name", flat=True)
        return bad_words

    def create_brand_safety_default_category_scores(self, data_type=constants.VIDEO):
        """
        Creates default brand safety category scores for video brand safety objects
            Default category brand safety scores not needed for channel brand safety objects since they are derived from videos
        :return: dict
        """
        allowed_data_types = {
            constants.VIDEO: 100,
            constants.CHANNEL: 0
        }
        try:
            default_category_score = allowed_data_types[data_type]
        except KeyError:
            raise ValueError("Unsupported data type for category scores: {}".format(data_type))
        categories = BadWordCategory.objects.exclude(id__in=self.bad_word_categories_ignore).values_list("id", flat=True)
        default_category_scores = {
            category_id: default_category_score
            for category_id in categories
        }
        return default_category_scores
