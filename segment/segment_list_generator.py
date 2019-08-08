from collections import defaultdict
import logging

from django.contrib.postgres.fields.jsonb import KeyTransform
from django.conf import settings

import brand_safety.constants as constants
from brand_safety.auditors.utils import AuditUtils
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles
from segment.utils import get_persistent_segment_connector_config_by_type


logger = logging.getLogger(__name__)


class SegmentListGenerator(object):
    CHANNEL_SDB_PARAM_FIELDS = "channel_id,title,thumbnail_image_url,category,subscribers,likes,dislikes,views,language"
    VIDEO_SDB_PARAM_FIELDS = "video_id,title,tags,thumbnail_image_url,category,likes,dislikes,views,language,transcript"
    CHANNEL_SCORE_FAIL_THRESHOLD = 89
    VIDEO_SCORE_FAIL_THRESHOLD = 89
    # Size of batch items retrieved from sdb
    CHANNEL_BATCH_LIMIT = 300
    VIDEO_BATCH_LIMIT = 600
    # Whitelist / blacklist requirements
    MINIMUM_CHANNEL_SUBSCRIBERS = 1000
    MINIMUM_VIDEO_VIEWS = 1000
    VIDEO_DISLIKE_RATIO_THRESHOLD = 0.2
    # Safe counter to break from video and channel generators
    CHANNEL_BATCH_COUNTER_LIMIT = 500
    VIDEO_BATCH_COUNTER_LIMIT = 500
    # Size of lists types
    WHITELIST_CHANNEL_SIZE = 100000
    BLACKLIST_CHANNEL_SIZE = 100000
    WHITELIST_VIDEO_SIZE = 100000
    BLACKLIST_VIDEO_SIZE = 100000
    # Actual list sizes used during runtime set by _set_config method
    BLACKLIST_SIZE = None
    WHITELIST_SIZE = None
    # Whether to sort items by views or subscribers for master segment related items
    RELATED_SEGMENT_SORT_KEY = None
    PK_NAME = None
    BATCH_LIMIT = None

    def __init__(self, *_, **kwargs):
        # If initialized with an APIScriptTracker instance, then expected to run full brand safety
        # else main run method should not be called since it relies on an APIScriptTracker instance
        try:
            self.script_tracker = kwargs["script_tracker"]
            self.cursor_id = self.script_tracker.cursor_id
            self.is_manual = False
        except KeyError:
            self.is_manual = True
        self.list_generator_type = kwargs["list_generator_type"]
        self.audit_utils = AuditUtils()
        self.channel_manager = ChannelManager(
            sections=(Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS),
            upsert_sections=(Sections.BRAND_SAFETY,)
        )
        self.video_manager = VideoManager(
            sections=(Sections.GENERAL_DATA, Sections.MAIN, Sections.STATS, Sections.CHANNEL, Sections.CAPTIONS),
            upsert_sections=(Sections.BRAND_SAFETY,)
        )
        self.master_blacklist_segment = None
        self.master_whitelist_segment = None
        self.segment_model = None
        self.related_segment_model = None
        self.evaluator = None

        self.batch_count = 0
        self._set_config()

    def _set_config(self):
        """
        Set configuration for script depending on list generation type, either channel or video
        :return:
        """
        if self.list_generator_type == constants.CHANNEL:
            if not self.is_manual:
                self.master_blacklist_segment, _ = PersistentSegmentChannel.objects.get_or_create(
                    title=PersistentSegmentTitles.CHANNELS_BRAND_SAFETY_MASTER_BLACKLIST_SEGMENT_TITLE,
                    category="blacklist")
                self.master_whitelist_segment, _ = PersistentSegmentChannel.objects.get_or_create(
                    title=PersistentSegmentTitles.CHANNELS_BRAND_SAFETY_MASTER_WHITELIST_SEGMENT_TITLE,
                    category="whitelist")

            # Config segments and models
            self.segment_model = PersistentSegmentChannel
            self.related_segment_model = PersistentSegmentRelatedChannel

            # Config methods
            self.data_generator = self._data_generator(self.channel_manager, cursor_id=self.cursor_id)
            self.evaluator = self._evaluate_channel

            # Config constants
            self.PK_NAME = "channel_id"
            self.SCORE_FAIL_THRESHOLD = self.CHANNEL_SCORE_FAIL_THRESHOLD
            self.BLACKLIST_SIZE = self.BLACKLIST_CHANNEL_SIZE
            self.WHITELIST_SIZE = self.WHITELIST_CHANNEL_SIZE
            self.INDEX_NAME = settings.BRAND_SAFETY_CHANNEL_INDEX
            self.BATCH_LIMIT = self.CHANNEL_BATCH_LIMIT
            self.RELATED_SEGMENT_SORT_KEY = "subscribers"

        elif self.list_generator_type == constants.VIDEO:
            if not self.is_manual:
                self.master_blacklist_segment, _ = PersistentSegmentVideo.objects.get_or_create(
                    title=PersistentSegmentTitles.VIDEOS_BRAND_SAFETY_MASTER_BLACKLIST_SEGMENT_TITLE,
                    category="blacklist")
                self.master_whitelist_segment, _ = PersistentSegmentVideo.objects.get_or_create(
                    title=PersistentSegmentTitles.VIDEOS_BRAND_SAFETY_MASTER_WHITELIST_SEGMENT_TITLE,
                    category="whitelist")

            # Config segments and models
            self.segment_model = PersistentSegmentVideo
            self.related_segment_model = PersistentSegmentRelatedVideo

            # Config methods
            self.data_generator = self._data_generator(self.video_manager, cursor_id=self.cursor_id)
            self.evaluator = self._evaluate_video

            # Config constants
            self.PK_NAME = "video_id"
            self.SCORE_FAIL_THRESHOLD = self.VIDEO_SCORE_FAIL_THRESHOLD
            self.BLACKLIST_SIZE = self.BLACKLIST_VIDEO_SIZE
            self.WHITELIST_SIZE = self.WHITELIST_VIDEO_SIZE
            self.INDEX_NAME = settings.BRAND_SAFETY_VIDEO_INDEX
            self.BATCH_LIMIT = self.VIDEO_BATCH_LIMIT
            self.RELATED_SEGMENT_SORT_KEY = "views"
        else:
            raise ValueError("Unsupported list generation type: {}".format(self.list_generator_type))

        self.unclassified_whitelist_manager = self.segment_model.objects.get(
            title=self.get_segment_title(self.segment_model.segment_type, "Unclassified",
                                         PersistentSegmentCategory.WHITELIST)
        )

    def run(self):
        """
        If initialized with an APIScriptTracker instance, then expected to run full brand safety
                else main run method should not be called since it relies on an APIScriptTracker instance
        :return: None
        """
        if self.is_manual:
            raise ValueError("SegmentListGenerator was not initialized with an APIScriptTracker instance.")
        for batch in self.data_generator:
            self._process(batch)
        logger.error("Complete. Cursor at: {}".format(self.script_tracker.cursor_id))

    def manual(self, items, segment_title, data_mapping):
        new_segment = self.segment_model.objects.create(
            title=segment_title,
            category=PersistentSegmentCategory.WHITELIST,
            is_master=False,
        )
        related_to_create = []
        for item in items:
            data = {
                related_key: item[data_key] for related_key, data_key in item.items()
            }
            related_to_create.append(self.related_segment_model(segment=new_segment, **data))
        self.related_segment_model.objects.bulk_create(related_to_create)

    def _process(self, sdb_items):
        """
        Drives main list generation logic
            Retrieves sdb data and es data, saves to category segments then master segments
        :param sdb_items:
        :return:
        """
        item_ids = [item[self.PK_NAME] for item in sdb_items]
        es_items = self._get_es_data(item_ids)
        # Update sdb data with es brand safety data
        merged_items = self._merge_data(es_items, sdb_items)
        sorted_by_category_whitelist = self._sort_by_category(merged_items)
        # Save items into their category segments
        for category, items in sorted_by_category_whitelist.items():
            clean_unclassified = False
            # For categories, only need to save to whitelists
            segment_title = self.get_segment_title(self.segment_model.segment_type, category, PersistentSegmentCategory.WHITELIST)
            try:
                whitelist_segment_manager = self.segment_model.objects.get(title=segment_title)
                # Whitelist found, remove items in current category from Unclassified
                clean_unclassified = True
            except self.segment_model.DoesNotExist:
                logger.error("Unable to get segment: {}".format(segment_title))
                logger.error("In unclassified: {}".format(
                    ["{}, {}".format(item[self.PK_NAME], item["category"]) for item in items]))
                whitelist_segment_manager = self.unclassified_whitelist_manager
            to_create = self.instantiate_related_items(items, whitelist_segment_manager)
            if clean_unclassified:
                self._clean(self.unclassified_whitelist_manager, to_create)
            self._clean(whitelist_segment_manager, to_create)
            self.related_segment_model.objects.bulk_create(to_create)
            self._truncate_list(whitelist_segment_manager, self.WHITELIST_SIZE)
        self._save_master_results(merged_items)

    def _sort_whitelist_blacklist(self, items):
        whitelist = []
        blacklist = []
        for item in items:
            passed = self.evaluator(item)
            if passed is True:
                whitelist.append(item)
            elif passed is False:
                blacklist.append(item)
            else:
                # If value of passed is not True nor False, then should not be added to any list
                pass
        return whitelist, blacklist

    def _evaluate_channel(self, channel):
        """
        Method to evaluate if a channel should be placed on a whitelist or blacklist
        :param channel: dict
        :return: bool
        """
        passed = None
        if channel.get("overall_score", 0) <= self.CHANNEL_SCORE_FAIL_THRESHOLD and channel.get("subscribers", 0) >= self.MINIMUM_CHANNEL_SUBSCRIBERS:
            passed = False
        else:
            if channel.get("subscribers", 0) >= self.MINIMUM_CHANNEL_SUBSCRIBERS:
                passed = True
        return passed

    def _evaluate_video(self, video):
        """
        Method to evaluate if a channel should be placed on a whitelist or blacklist
        :param video: dict
        :return: bool
        """
        passed = None
        if video.get("overall_score", 0) <= self.VIDEO_SCORE_FAIL_THRESHOLD and video.get("views", 0) >= self.MINIMUM_VIDEO_VIEWS:
            passed = False
        else:
            try:
                likes = int(video.get("likes", 0))
                dislikes = int(video.get("dislikes", 0))
                dislike_ratio = dislikes / (likes + dislikes)
            except (ValueError, ZeroDivisionError):
                dislike_ratio = 1
            if video.get("views", 0) >= self.MINIMUM_VIDEO_VIEWS and dislike_ratio < self.VIDEO_DISLIKE_RATIO_THRESHOLD:
                passed = True
        return passed

    def _merge_data(self, es_data, sdb_items):
        """
        Update sdb data with es brand safety data
        :param es_data: dict
        :param sdb_items: list
        :return: list
        """
        for item in sdb_items:
            item_id = item[self.PK_NAME]
            es_item = es_data.get(item_id)
            if es_item is None:
                continue
            keywords = self._extract_keywords(es_item)
            item[constants.BRAND_SAFETY_HITS] = keywords
            item["overall_score"] = es_item["overall_score"]
            try:
                item["audited_videos"] = es_item["videos_scored"]
            except KeyError:
                pass
        return sdb_items

    def _sort_by_category(self, items):
        """
        Sort objects by their category
        :param items: sdb data
        :return: list
        """
        items_by_category = defaultdict(list)
        for item in items:
            category = item["category"].strip()
            whitelist_pass = self.evaluator(item)
            if whitelist_pass is True:
                items_by_category[category].append(item)
        return items_by_category

    def _save_master_results(self, items):
        """
        Save Video and Channel audits based on their brand safety results to their respective persistent segments
        :param kwargs: Persistent segments to save to
        :return: None
        """
        # Sort audits by brand safety results and truncate master lists
        whitelist_items, blacklist_items = self._sort_whitelist_blacklist(items)
        blacklist_to_create = self.instantiate_related_items(blacklist_items, self.master_blacklist_segment)
        self._clean(self.master_blacklist_segment, blacklist_to_create)
        self.related_segment_model.objects.bulk_create(blacklist_to_create)
        self._truncate_list(self.master_blacklist_segment, self.BLACKLIST_SIZE)

        whitelist_to_create = self.instantiate_related_items(whitelist_items, self.master_whitelist_segment)
        self._clean(self.master_whitelist_segment, whitelist_to_create)
        self.related_segment_model.objects.bulk_create(whitelist_to_create)
        self._truncate_list(self.master_whitelist_segment, self.WHITELIST_SIZE)

    @staticmethod
    def get_segment_title(segment_type, category, segment_category):
        """
        Return formatted Persistent segment title
        :param segment_type: channel or video
        :param category: Item category e.g. Politics
        :param segment_category: whitelist or blacklist
        :return:
        """
        categorized_segment_title = "{}s {} Brand Suitability {}".format(
            segment_type.capitalize(),
            category,
            segment_category.capitalize(),
        )
        return categorized_segment_title

    def instantiate_related_items(self, items, segment_manager):
        """
        Instantiate related objects
        :param items: sdb data merged with es data
        :param segment_manager: target persistent segment
        :return: list
        """
        to_create = []
        for item in items:
            # Ignore items that do not have brand safety data
            if item.get("overall_score") is None:
                continue
            related_obj = self.related_segment_model(
                related_id=item[self.PK_NAME],
                segment=segment_manager,
                title=item["title"],
                category=item["category"],
                thumbnail_image_url=item["thumbnail_image_url"],
                details={
                    "language": item["language"],
                    "likes": item["likes"],
                    "dislikes": item["dislikes"],
                    "views": item["views"],
                    "bad_words": item.get(constants.BRAND_SAFETY_HITS, []),
                    "overall_score": item.get("overall_score", "Unavailable")
                }
            )
            if segment_manager.segment_type == constants.CHANNEL:
                related_obj.details["subscribers"] = item["subscribers"]
                related_obj.details["audited_videos"] = item.get("audited_videos")
            to_create.append(related_obj)
        return to_create

    def _get_es_data(self, item_ids):
        """
        Wrapper to encapsulate es data retrieval
        :param item_ids:
        :return:
        """
        response = self.es_connector.search_by_id(self.INDEX_NAME, item_ids, settings.BRAND_SAFETY_TYPE)
        return response

    def _get_sdb_data(self, item_ids, item_type):
        """
        Wrapper to retrieve sdb data
        :param item_ids: channel / video ids
        :param item_type: channel / video
        :return: dict
        """
        config = get_persistent_segment_connector_config_by_type(item_type, item_ids)
        config["fields"] = self.CHANNEL_SDB_PARAM_FIELDS if item_type == constants.CHANNEL else self.VIDEO_SDB_PARAM_FIELDS
        connector_method = config.pop("method")
        response = connector_method(config)
        sdb_data = {
            item[item_type + "_id"]: item for item in response.get("items")
        }
        return sdb_data

    def _extract_keywords(self, es_doc):
        """
        Extract nested keyword hits from es brand safety data
        :param es_doc:
        :return: list
        """
        all_keywords = set()
        for category_data in es_doc["categories"].values():
            keywords = [word["keyword"] for word in category_data["keywords"]]
            all_keywords.update(keywords)
        return list(all_keywords)

    def _clean(self, segment_manager, items):
        """
        Clean related segment model for duplicate ids
        :param items: list -> PersistentSegmentRelated items
        :return: None
        """
        item_ids = [item.related_id for item in items]
        segment_manager.related.filter(related_id__in=item_ids).delete()

    def _data_generator(self, manager, cursor_id=None):
        cursor_id = cursor_id or ""
        while self.batch_count <= self.CHANNEL_BATCH_COUNTER_LIMIT:
            batch_query = QueryBuilder().build().must().range().field(MAIN_ID_FIELD).gte(cursor_id).get()
            response = self.channel_manager.search(batch_query, limit=self.CHANNEL_BATCH_LIMIT).execute()
            results = response["hits"]["hits"]
            if not results:
                self.audit_utils.set_cursor(self.script_tracker, None, integer=False)
                break
            yield list(results)
            cursor_id = results[-1]["_id"]
            self.script_tracker = self.audit_utils.set_cursor(self.script_tracker, cursor_id, integer=False)
            self.cursor_id = self.script_tracker.cursor_id
            self.batch_count += 1

    def _truncate_list(self, segment, size):
        """
        Truncate master segments
        :param segment: PersistentSegment model
        :return: None
        """
        sort_key = self.RELATED_SEGMENT_SORT_KEY
        annotate_config = {
            constants.CHANNEL: {
                "subscribers": KeyTransform(sort_key, "details")
            },
            constants.VIDEO: {
                "views": KeyTransform(sort_key, "details")
            }
        }
        annotation = annotate_config[segment.segment_type]
        related_ids_to_truncate = segment.related.annotate(**annotation).order_by("-{}".format(sort_key)).values_list("related_id", flat=True)[size:]
        segment.related.filter(related_id__in=list(related_ids_to_truncate)).delete()

    def _set_defaults(self, items):
        """
        Set default values for items
        :param items: list
        :return: None
        """
        for item in items:
            if not item.get("category"):
                item["category"] = "Unclassified"
            if not item.get("language"):
                item["language"] = "Unknown"
