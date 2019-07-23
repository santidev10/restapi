from collections import Counter
from collections import defaultdict
from datetime import datetime
import logging
import multiprocessing as mp
from time import sleep

from django.db.models import F
from django.conf import settings
from flashtext import KeywordProcessor

from brand_safety import constants
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from brand_safety.audit_providers.base import AuditProvider
from brand_safety.audit_services.standard_brand_safety_service import StandardBrandSafetyService
from singledb.connector import SingleDatabaseApiConnector
from utils.elasticsearch import ElasticSearchConnector

logger = logging.getLogger(__name__)


class StandardBrandSafetyProvider(object):
    """
    Interface for reading source data and providing it to services
    """
    max_process_count = 4
    channel_id_pool_batch_limit = 10
    channel_id_master_batch_limit = max_process_count * channel_id_pool_batch_limit
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
    DEFAULT_SLEEP = 1.5

    def __init__(self, *_, **kwargs):
        # If initialized with an APIScriptTracker instance, then expected to run full brand safety
        # else main run method should not be called since it relies on an APIScriptTracker instance
        try:
            self.script_tracker = kwargs["api_tracker"]
            self.cursor_id = self.script_tracker.cursor_id
            self.is_manual = False
        except KeyError:
            self.is_manual = True
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
            es_video_index=settings.BRAND_SAFETY_VIDEO_INDEX,
            es_channel_index=settings.BRAND_SAFETY_CHANNEL_INDEX
        )
        self.es_connector = ElasticSearchConnector()

    def run(self):
        """
        Pools processes to handle main audit logic and processes results
            If initialized with an APIScriptTracker instance, then expected to run full brand safety
                else main run method should not be called since it relies on an APIScriptTracker instance
        :return: None
        """
        if self.is_manual:
            raise ValueError("Provider was not initialized with an APIScriptTracker instance.")
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
        logger.error("Complete. Cursor at: {}".format(self.script_tracker.cursor_id))

    def _process_audits(self, channel_ids):
        """
        Drives main brand safety logic for each process
        :param channel_ids: Channel ids to retrieve video data for
        :return:
        """
        sleep(self.DEFAULT_SLEEP)
        video_audits = self.audit_service.audit_videos(channel_ids=channel_ids)
        print("got video audits", len(video_audits))
        sorted_video_audits = self.audit_service.sort_video_audits(video_audits)
        sleep(self.DEFAULT_SLEEP)
        channel_audits = self.audit_service.audit_channels(sorted_video_audits)

        print('got channel auits', len(channel_audits))
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
            index_name=settings.BRAND_SAFETY_VIDEO_INDEX
        )
        self._index_brand_safety_results(
            channel_audits,
            index_name=settings.BRAND_SAFETY_CHANNEL_INDEX
        )

    def _index_brand_safety_results(self, results, index_name):
        index_type = settings.BRAND_SAFETY_TYPE
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
                # No more items to process, reset cursor
                self.audit_provider.set_cursor(self.script_tracker, None, integer=False)
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

    def get_bad_word_processors_by_language(self):
        """
        Generate dictionary of keyword processors by language
            Also provides an "all" key that contains every keyword
        :return:
        """
        bad_words_by_language = defaultdict(KeywordProcessor)
        all_words = BadWord.objects.annotate(language_name=F("language__language"))
        for word in all_words:
            language = word.language_name
            bad_words_by_language["all"].add_keyword(word.name)
            bad_words_by_language[language].add_keyword(word.name)
        # Cast back to dictionary to avoid creation of new keys
        bad_words_by_language = dict(bad_words_by_language)
        return bad_words_by_language

    def manual_channel_update(self, channel_ids: iter):
        """
        Update specific channels and videos
        :param channel_ids: list | tuple
        :return: None
        """
        self.channel_id_pool_batch_limit = 3
        pool = mp.Pool(processes=self.max_process_count)
        results = pool.map(self._process_audits, self.audit_provider.batch(channel_ids, self.channel_id_pool_batch_limit))

        # Extract nested results from each process and index into es
        video_audits, channel_audits = self._extract_results(results)
        self._index_results(video_audits, channel_audits)
        return channel_audits

    def manual_video_update(self, video_ids: iter):
        """
        Manually add / update brand safety video ids
        :param video_ids: list | tuple -> Youtube video id strings
        :return: BrandSafetyVideoAudit objects
        """
        video_data = self.audit_service.get_video_data(video_ids)
        video_audits = self.audit_service.audit_videos(video_data)
        self._index_results(video_audits, [])
        return video_audits

    def _get_channels_to_update(self, channel_ids):
        """
        Gets channels to update
            If either the last time the channel has been updated is greater than threshold time or if the number of videos
            has changed since the last time the channel was scored, it should be upated
        :param channel_ids: list
        :return: list
        """
        channels_to_update = []
        channel_es_data = self._get_channel_es_data(channel_ids)
        channel_ids_from_videos = self.audit_service.get_channel_video_data(channel_ids, fields="channel_id")
        channel_video_counts = Counter([item["channel_id"] for item in channel_ids_from_videos if item.get("channel_id")])
        for _id in channel_ids:
            try:
                es_data = channel_es_data[_id]
                # If elapsed time since channel has been updated is greater than threshold or if the number of videos
                # last scored is different than the current number of channel's videos, then should be updated
                if es_data["should_update"] is True or channel_video_counts[_id] != es_data["videos_scored"]:
                    channels_to_update.append(_id)
            except KeyError:
                channels_to_update.append(_id)
        return channels_to_update

    def _get_channel_es_data(self, channel_ids):
        """
        Get Elasticsearch channels to check when channels were last updated
        :param channel_ids: list
        :return: list
        """
        all_data = {}
        es_channels = self.es_connector.search_by_id(
            settings.BRAND_SAFETY_CHANNEL_INDEX,
            channel_ids,
            settings.BRAND_SAFETY_TYPE
        )
        if not es_channels:
            return all_data
        for _id in channel_ids:
            channel_data = {
                "should_update": True,
                "videos_scored": 0,
            }
            try:
                es_channel = es_channels[_id]
                # check if any videos have been added
                time_elapsed = datetime.today() - datetime.strptime(es_channel["updated_at"], "%Y-%m-%d")
                elapsed_hours = time_elapsed.seconds // 3600
                channel_data["videos_scored"] = es_channel["videos_scored"]
                if not elapsed_hours > self.update_time_threshold:
                    channel_data["should_update"] = False
            except KeyError:
                # If channel is not in index or has no updated_at, create / update it
                pass
            all_data[_id] = channel_data
        return all_data
