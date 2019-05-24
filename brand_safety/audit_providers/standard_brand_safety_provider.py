import logging
from datetime import datetime
from collections import defaultdict
import multiprocessing as mp

from flashtext import KeywordProcessor

from utils.elasticsearch import ElasticSearchConnector
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
# from brand_safety.models import BadWordLanguage
from brand_safety import constants
from brand_safety.audit_providers.base import AuditProvider
from brand_safety.audit_services.standard_brand_safety_service import StandardBrandSafetyService
from singledb.connector import SingleDatabaseApiConnector

logger = logging.getLogger(__name__)


class StandardBrandSafetyProvider(object):
    """
    Interface for reading source data and providing it to services
    """
    channel_id_master_batch_limit = 240
    channel_id_pool_batch_limit = 30
    max_process_count = 8
    # Multiplier to apply for brand safety hits
    brand_safety_score_multiplier = {
        "title": 4,
        "description": 1,
        "tags": 1,
        "transcript": 1,
    }
    channel_batch_counter = 0
    channel_batch_counter_limit = 500
    # Hours in which a channel should be updated
    update_time_threshold = 24 * 7

    def __init__(self, *_, **kwargs):
        self.script_tracker = kwargs["api_tracker"]
        self.cursor_id = self.script_tracker.cursor_id
        self.audit_provider = AuditProvider()
        self.sdb_connector = SingleDatabaseApiConnector()
        # Audit mapping for audit objects to use
        self.audits = {
            constants.BRAND_SAFETY: self.get_bad_word_processors_by_language(),
            constants.EMOJI: self.audit_provider.compile_emoji_regexp()
        }
        # Initial category brand safety scores for videos and channels, since ignoring certain categories (e.g. Kid's Content)
        self.default_video_category_scores = self._create_brand_safety_default_category_scores(data_type=constants.VIDEO)
        self.default_channel_category_scores = self._create_brand_safety_default_category_scores(data_type=constants.CHANNEL)
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
        for channel_batch in self._channel_id_batch_generator(self.cursor_id):
            if not channel_batch:
                continue
            # Update score mapping so each batch uses updated brand safety scores
            results = pool.map(self._process_audits, self.audit_provider.batch(channel_batch, self.channel_id_pool_batch_limit))

            # Extract nested results from each process and index into es
            video_audits, channel_audits = self._extract_results(results)
            self._index_results(video_audits, channel_audits)

            # Update brand safety scores in case they have been modified since last batch
            self.audit_service.score_mapping = self.get_brand_safety_score_mapping()
            # Update brand safety processors
            self.audits[constants.BRAND_SAFETY] = self.get_bad_word_processors_by_language()
            self.audit_service.audits = self.audits
        self.audit_provider.set_cursor(self.script_tracker, None, integer=False)

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

    def _index_results(self, video_audits, channel_audits):
        """
        Sends request to index results in Elasticsearch and save to db segments
        :param video_audits:
        :param channel_audits:
        :return:
        """
        self._index_brand_safety_results(
            video_audits,
            index_name=constants.BRAND_SAFETY_VIDEO_ES_INDEX
        )
        self._index_brand_safety_results(
            channel_audits,
            index_name=constants.BRAND_SAFETY_CHANNEL_ES_INDEX
        )

    def _index_brand_safety_results(self, results, index_name):
        index_type = "score"
        op_type = "index"
        es_bulk_generator = (audit.es_repr(index_name, index_type, op_type) for audit in results)
        self.es_connector.push_to_index(es_bulk_generator)

    def _channel_id_batch_generator(self, cursor_id):
        """
        Yields batch channel ids to audit
        :param cursor_id: Cursor position to start audit
        :return: list -> Youtube channel ids
        """
        params = {
            "fields": "channel_id",
            "sort": "channel_id",
            "size": self.channel_id_master_batch_limit,
        }
        while self.channel_batch_counter <= self.channel_batch_counter_limit:
            params["channel_id__range"] = "{},".format(cursor_id or "")
            response = self.sdb_connector.get_channel_list(params, ignore_sources=True)
            channel_ids = [item["channel_id"] for item in response.get("items", []) if item["channel_id"] != cursor_id]
            if not channel_ids:
                break
            channels_to_update = self._get_channels_to_update(channel_ids)
            yield channels_to_update
            cursor_id = channel_ids[-1]
            # Update script tracker and cursors
            self.script_tracker = self.audit_provider.set_cursor(self.script_tracker, cursor_id, integer=False)
            self.cursor_id = self.script_tracker.cursor_id
            self.channel_batch_counter += 1

    @staticmethod
    def get_brand_safety_score_mapping():
        """
        Map brand safety BadWord rows to their score
        :return: dict
        """
        # TODO: Need to separate the words into their respective languages
        score_mapping = defaultdict(dict)
        for word in BadWord.objects.all():
            score_mapping[word.name] = {
                "category": word.category_id,
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
            .values_list("name", flat=True)
        return bad_words

    def _create_brand_safety_default_category_scores(self, data_type=constants.VIDEO):
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
        categories = BadWordCategory.objects.values_list("id", flat=True)
        default_category_scores = {
            category_id: default_category_score
            for category_id in categories
        }
        return default_category_scores

    def _get_channels_to_update(self, channel_ids):
        """
        Get Elasticsearch channels to check when channels were last updated
            and filter for channels to update
        :param channel_ids: list
        :return: list
        """
        channels_to_update = []
        es_channels = self.es_connector.search_by_id(
            constants.BRAND_SAFETY_CHANNEL_ES_INDEX,
            channel_ids,
            constants.BRAND_SAFETY_SCORE_TYPE
        )
        if not es_channels:
            return channel_ids
        for channel in channel_ids:
            try:
                es_channel = es_channels[channel]
                time_elapsed = datetime.today() - datetime.strptime(es_channel["updated_at"], "%Y-%m-%d")
                elapsed_hours = time_elapsed.seconds // 3600
                if elapsed_hours > self.update_time_threshold:
                    channels_to_update.append(channel)
            except KeyError:
                # If channel is not in index or has no updated_at, create / update it
                channels_to_update.append(channel)
        return channels_to_update

    def get_bad_word_processors_by_language(self):
        bad_words_by_language = defaultdict(KeywordProcessor)
        for language in BadWordLanguage:
            language_words = language.words.all().values_list("name", flat=True)
            bad_words_by_language["all"].add_keywords_from_list(language_words)
            bad_words_by_language[language.name].add_keywords_from_list(language_words)
        return bad_words_by_language



