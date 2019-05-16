import logging
from datetime import datetime
from collections import defaultdict
import multiprocessing as mp

from django.db.models import Q

from utils.elasticsearch import ElasticSearchConnector
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from brand_safety import constants
from brand_safety.audit_providers.base import AuditProvider
from brand_safety.audit_services.standard_brand_safety_service import StandardBrandSafetyService
from singledb.connector import SingleDatabaseApiConnector
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles

logger = logging.getLogger(__name__)


class StandardBrandSafetyProvider(object):
    """
    Interface for reading source data and providing it to services
    """
    channel_id_master_batch_limit = 3
    channel_id_pool_batch_limit = 3
    # channel_id_master_batch_limit = 500
    # channel_id_pool_batch_limit = 50
    max_process_count = 3
    brand_safety_fail_threshold = 3
    # Multiplier to apply for brand safety hits
    brand_safety_score_multiplier = {
        "title": 4,
        "description": 1,
        "tags": 1,
        "transcript": 1,
    }
    # Bad words in these categories should be ignored while calculating brand safety scores
    bad_word_categories_ignore = [9]
    channel_batch_counter = 0
    channel_batch_counter_limit = 500
    update_time_threshold = 7

    def __init__(self, *_, **kwargs):
        self.script_tracker = kwargs["api_tracker"]
        self.cursor_id = self.script_tracker.cursor_id
        self.audit_provider = AuditProvider()
        self.sdb_connector = SingleDatabaseApiConnector()
        # Audit mapping for audit objects to use
        self.audits = {
            constants.BRAND_SAFETY: self.audit_provider.get_trie_keyword_processor(self.get_bad_words()),
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
        self._set_master_segments()

    def run(self):
        """
        Pools processes to handle main audit logic and processes results
        :return: None
        """
        logger.info("Starting standard audit...")
        pool = mp.Pool(processes=self.max_process_count)
        for channel_batch in self._channel_id_batch_generator(self.cursor_id):
            # Update score mapping so each batch uses updated brand safety scores
            results = pool.map(self._process_audits, self.audit_provider.batch(channel_batch, self.channel_id_pool_batch_limit))

            # Extract nested results from each process
            video_audits, channel_audits = self._extract_results(results)
            self._index_results(video_audits, channel_audits)

            # Process saving video and channel audits into segments separately as they must be saved to different segments
            self._save_master_results(
                audits=video_audits, whitelist_segment=self.whitelist_videos,
                blacklist_segment=self.blacklist_videos,
                related_segment_model=PersistentSegmentRelatedVideo)

            self._save_master_results(
                audits=channel_audits, whitelist_segment=self.whitelist_channels,
                blacklist_segment=self.blacklist_channels, related_segment_model=PersistentSegmentRelatedChannel)

            # Save to category whitelists
            self._save_category_results(video_audits, PersistentSegmentVideo, PersistentSegmentRelatedVideo)
            self._save_category_results(channel_audits, PersistentSegmentChannel, PersistentSegmentRelatedChannel)

            # Update script tracker and cursors
            self.script_tracker = self.audit_provider.set_cursor(self.script_tracker, channel_batch[-1], integer=False)
            self.cursor_id = self.script_tracker.cursor_id

            # Update brand safety scores in case they have been modified since last batch
            self.audit_service.score_mapping = self.get_brand_safety_score_mapping()
        logger.info("Standard Brand Safety Audit Complete.")
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
            # self.audit_service.gather_brand_safety_results(video_audits),
            video_audits,
            index_name=constants.BRAND_SAFETY_VIDEO_ES_INDEX
        )
        self._index_brand_safety_results(
            # self.audit_service.gather_brand_safety_results(channel_audits),
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
            channels = [item["channel_id"] for item in response.get("items", []) if item["channel_id"] != cursor_id]
            if not channels:
                break
            channels = self._get_channels_to_update(channels)
            yield channels
            cursor_id = channels[-1]
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

    def _save_category_results(self, audits, segment_model, related_segment_model):
        """
        Save audits into their respective category segments
            Category segments only contain whitelists
        :param audits: Channel or Video audit objects
        :param segment_model: PersistentSegmentModel
        :param releated_segment_model: PersistentSegmentRelated Model
        :return:
        """
        # sort audits by their categories
        audits_by_category = defaultdict(lambda: defaultdict(list))
        for audit in audits:
            category = audit.metadata["category"]
            if audit.target_segment == PersistentSegmentCategory.BLACKLIST:
                audits_by_category[category]["fail"].append(audit)
            else:
                audits_by_category[category]["pass"].append(audit)

        # Remove existing ids to create
        for category, audits in audits_by_category.items():
            try:
                segment_title = self._get_segment_title(
                    segment_model.segment_type,
                    category,
                    PersistentSegmentCategory.WHITELIST,
                )
                print(segment_title)
                whitelist_segment_manager, _ = segment_model.objects.get_or_create(title=segment_title)
                passed_related_ids = [audit.pk for audit in audits["pass"]]
                failed_related_ids = [audit.pk for audit in audits["fail"]]
                # Delete items that have failed from segment whitelist
                whitelist_segment_manager.related.filter(related_id__in=failed_related_ids).delete()
                # Set difference for existing and to create to prevent creating duplicates
                existing_passed = whitelist_segment_manager.related.filter(related_id__in=passed_related_ids)
                to_create = set(existing_passed) - set(passed_related_ids)
                related_segment_model.objects.bulk_create(to_create)
            except segment_model.DoesNotExist:
                print("Unable to get category segment: {}".format(category))
                raise

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
        categories = BadWordCategory.objects.exclude(id__in=self.bad_word_categories_ignore).values_list("id", flat=True)
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
            constants.BRAND_SAFETY_SCORE
        )
        if not es_channels:
            return channel_ids
        for channel in channel_ids:
            try:
                es_channel = es_channels[channel]
                time_elapsed = datetime.today() - datetime.strptime(es_channel["updated_at"], "%Y-%m-%d")
                if time_elapsed.days > self.update_time_threshold:
                    channels_to_update.append(channel)
            except KeyError:
                # If channel is not in index or has no updated_at, create / update it
                channels_to_update.append(channel)
        return channels_to_update

    def _set_master_segments(self):
        """
        Set required persistent segments to save to
        :return:
        """
        self.blacklist_channels, _ = PersistentSegmentChannel.objects.get_or_create(
            title=PersistentSegmentTitles.CHANNELS_BRAND_SAFETY_MASTER_BLACKLIST_SEGMENT_TITLE, category="blacklist")
        self.whitelist_channels, _ = PersistentSegmentChannel.objects.get_or_create(
            title=PersistentSegmentTitles.CHANNELS_BRAND_SAFETY_MASTER_WHITELIST_SEGMENT_TITLE, category="whitelist")
        self.blacklist_videos, _ = PersistentSegmentVideo.objects.get_or_create(
            title=PersistentSegmentTitles.VIDEOS_BRAND_SAFETY_MASTER_BLACKLIST_SEGMENT_TITLE, category="blacklist")
        self.whitelist_videos, _ = PersistentSegmentVideo.objects.get_or_create(
            title=PersistentSegmentTitles.VIDEOS_BRAND_SAFETY_MASTER_WHITELIST_SEGMENT_TITLE, category="whitelist")

    def _sort_brand_safety(self, audits):
        """
        Sort audits by fail or pass based on their overall brand safety score
        :param audits: list
        :return: tuple -> lists of sorted audits
        """
        brand_safety_pass = {}
        brand_safety_fail = {}
        for audit in audits:
            if audit.target_segment == PersistentSegmentCategory.WHITELIST:
                brand_safety_pass[audit.pk] = audit
            elif audit.target_segment == PersistentSegmentCategory.BLACKLIST:
                brand_safety_fail[audit.pk] = audit
            else:
                pass
        return brand_safety_pass, brand_safety_fail

    def _save_master_results(self, *_, **kwargs):
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
        # Get related ids that still exist to avoid creating duplicates with results
        exists = related_segment_model.objects \
            .filter(
            Q(segment=whitelist_segment) | Q(segment=blacklist_segment),
            related_id__in=brand_safety_pass_pks + brand_safety_fail_pks
        ).values_list("related_id", flat=True)
        # Set difference to get audits that need to be created
        to_create = set(brand_safety_pass_pks + brand_safety_fail_pks) - set(exists)
        # Instantiate related models with new appropriate segment and segment types
        to_create = [
            brand_safety_pass[pk].instantiate_related_model(related_segment_model, whitelist_segment,
                                                            segment_type=constants.WHITELIST)
            if brand_safety_pass.get(pk) is not None
            else
            brand_safety_fail[pk].instantiate_related_model(related_segment_model, blacklist_segment,
                                                            segment_type=constants.BLACKLIST)
            for pk in to_create
        ]
        related_segment_model.objects.bulk_create(to_create)
        return brand_safety_pass, brand_safety_fail

    @staticmethod
    def _get_segment_title(segment_type, category, segment_category):
        """
        Return formatted Persistent segment title
        :param segment_type: channel or video
        :param category: Item category e.g. Politics
        :param segment_category: whitelist or blacklist
        :return:
        """
        categorized_segment_title = "{}s {} {}".format(
            segment_type.capitalize(),
            category,
            segment_category.capitalize(),
        )
        return categorized_segment_title



