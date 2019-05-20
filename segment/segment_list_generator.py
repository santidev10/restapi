from collections import defaultdict

import brand_safety.constants as constants
from brand_safety.audit_providers.base import AuditProvider
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles
from segment.utils import get_persistent_segment_connector_config_by_type
from singledb.connector import SingleDatabaseApiConnector
from utils.elasticsearch import ElasticSearchConnector


class SegmentListGenerator(object):
    CHANNEL_SCORE_FAIL_THRESHOLD = 0
    VIDEO_SCORE_FAIL_THRESHOLD = 89
    ES_SIZE = 50
    CHANNEL_SDB_PARAM_FIELDS = "channel_id,title,thumbnail_image_url,category,subscribers,likes,dislikes,views,language"
    VIDEO_SDB_PARAM_FIELDS = "video_id,title,tags,thumbnail_image_url,category,likes,dislikes,views,language,transcript"
    # TEST
    CHANNEL_BATCH_LIMIT = 2
    # CHANNEL_BATCH_LIMIT = 100
    VIDEO_BATCH_LIMIT = 100
    MINIMUM_CHANNEL_SUBSCRIBERS = 1000
    MINIMUM_VIDEO_VIEWS = 1000
    CHANNEL_BATCH_COUNTER_LIMIT = 500
    MASTER_WHITELIST_CHANNEL_SIZE = 20000
    MASTER_BLACKLIST_CHANNEL_SIZE = 20000
    MASTER_WHITELIST_VIDEO_SIZE = 20000
    MASTER_BLACKLIST_VIDEO_SIZE = 20000
    MASTER_BLACKLIST_SIZE = None
    MASTER_WHITELIST_SIZE = None
    PK_NAME = None

    def __init__(self, *_, **kwargs):
        self.api_script_tracker = kwargs["api_script_tracker"]
        self.list_generator_type = kwargs["list_generator_type"]
        self.cursor_id = self.api_script_tracker.cursor_id
        self.sdb_connector = SingleDatabaseApiConnector()
        self.es_connector = ElasticSearchConnector()
        self.audit_provider = AuditProvider()

        self.sdb_data_generator = None
        self.master_blacklist_segment = None
        self.master_whitelist_segment = None
        self.segment_model = None
        self.related_segment_model = None
        self.evaluator = None

        self.BATCH_LIMIT = None
        self.batch_count = 0
        self._set_config()

    def _set_config(self):
        """
        Set configuration depending on list generation type
        :return:
        """
        if self.list_generator_type == constants.CHANNEL:
            # Config constants
            self.PK_NAME = "channel_id"
            self.SCORE_FAIL_THRESHOLD = self.CHANNEL_SCORE_FAIL_THRESHOLD
            self.MASTER_BLACKLIST_SIZE = self.MASTER_BLACKLIST_CHANNEL_SIZE
            self.MASTER_WHITELIST_SIZE = self.MASTER_WHITELIST_CHANNEL_SIZE
            self.BATCH_LIMIT = self.CHANNEL_BATCH_LIMIT

            # Config segments and models
            self.segment_model = PersistentSegmentChannel
            self.related_segment_model = PersistentSegmentRelatedChannel
            self.evaluator = self._evaluate_channel

            self.master_blacklist_segment, _ = PersistentSegmentChannel.objects.get_or_create(
                title=PersistentSegmentTitles.CHANNELS_BRAND_SAFETY_MASTER_BLACKLIST_SEGMENT_TITLE,
                category="blacklist")
            self.master_whitelist_segment, _ = PersistentSegmentChannel.objects.get_or_create(
                title=PersistentSegmentTitles.CHANNELS_BRAND_SAFETY_MASTER_WHITELIST_SEGMENT_TITLE,
                category="whitelist")

            # Config methods
            self.sdb_data_generator = self._channel_batch_generator


            self.index_name = constants.BRAND_SAFETY_CHANNEL_ES_INDEX


        elif self.list_generator_type == constants.VIDEO:
            self.MASTER_BLACKLIST_SIZE = self.MASTER_BLACKLIST_VIDEO_SIZE
            self.MASTER_WHITELIST_SIZE = self.MASTER_WHITELIST_VIDEO_SIZE
            self.BATCH_LIMIT = self.VIDEO_BATCH_LIMIT
            self.sdb_data_generator = self._video_batch_generator
            self.segment_model = PersistentSegmentVideo
            self.related_segment_model = PersistentSegmentRelatedVideo
            self.PK_NAME = "video_id"
            self.evaluator = self._evaluate_video
            self.index_name = constants.BRAND_SAFETY_VIDEO_ES_INDEX
            self.master_blacklist_segment, _ = PersistentSegmentVideo.objects.get_or_create(
                title=PersistentSegmentTitles.VIDEOS_BRAND_SAFETY_MASTER_BLACKLIST_SEGMENT_TITLE, category="blacklist")
            self.master_whitelist_segment, _ = PersistentSegmentVideo.objects.get_or_create(
                title=PersistentSegmentTitles.VIDEOS_BRAND_SAFETY_MASTER_WHITELIST_SEGMENT_TITLE, category="whitelist")
        else:
            raise ValueError("Unsupported list generation type: {}".format(self.list_generator_type))

    def run(self):
        for batch in self.sdb_data_generator(self.cursor_id):
            # If this condition is True, then script was interrupted in the middle of this batch
                # Remove any items from this batch from potentially raising a db Integrity error
            if self.cursor_id is not None and self.batch_count == 0:
                print('cleaning')
                self._clean(batch)
            self._proocess(batch)
            break
        # Done here
        print('done')

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
        passed = None
        if channel["overall_score"] <= self.CHANNEL_SCORE_FAIL_THRESHOLD:
            passed = False
        else:
            if channel.get("subscribers", 0) >= self.MINIMUM_CHANNEL_SUBSCRIBERS:
                passed = True
        return passed

    def _evaluate_video(self, video):
        passed = None
        if video["overall_score"] <= self.VIDEO_SCORE_FAIL_THRESHOLD:
            passed = False
        else:
            if video.get("views", 0) >= self.MINIMUM_VIDEO_VIEWS:
                passed = True
        return passed

    def _proocess(self, sdb_items):
        item_ids = [item[self.PK_NAME] for item in sdb_items]
        es_items = self._get_es_data(item_ids)
        merged_items = self._merge_data(es_items, sdb_items)
        sorted_by_category_whitelist = self._sort_by_category(merged_items)
        # Save items into their category segments
        for category, items in sorted_by_category_whitelist.items():
            segment_title = self._get_segment_title(self.segment_model.segment_type, category, PersistentSegmentCategory.WHITELIST)
            try:
                whitelist_segment_manager = self.segment_model.objects.get(title=segment_title)
                to_create = self._instantiate_related_items(items, whitelist_segment_manager)
                self.related_segment_model.objects.bulk_create(to_create)
            except self.segment_model.DoesNotExist:
                print("Unable to get segment: {}".format(segment_title))
            break
        self._save_master_results(merged_items)

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
            category = item["category"]
            items_by_category[category].append(item)
        return items_by_category

    def _save_master_results(self, items):
        """
        Save Video and Channel audits based on their brand safety results to their respective persistent segments
        :param kwargs: Persistent segments to save to
        :return: None
        """
        # Sort audits by brand safety results
        whitelist_items, blacklist_items = self._sort_whitelist_blacklist(items)
        if self.master_blacklist_segment.related.count() <= self.MASTER_BLACKLIST_SIZE:
            blacklist_to_create = self._instantiate_related_items(blacklist_items, self.master_blacklist_segment)
            self.related_segment_model.objects.bulk_create(blacklist_to_create)

        if self.master_whitelist_segment.related.count() <= self.MASTER_WHITELIST_SIZE:
            whitelist_to_create = self._instantiate_related_items(whitelist_items, self.master_whitelist_segment)
            self.related_segment_model.objects.bulk_create(whitelist_to_create)

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

    def _instantiate_related_items(self, items, segment_manager):
        """
        Instantiate related objects
        :param items: sdb data merged with es data
        :param segment_manager: target persistent segment
        :return: list
        """
        to_create = []
        for item in items:
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
                    "bad_words": item[constants.BRAND_SAFETY_HITS],
                    "overall_score": item["overall_score"]
                }
            )
            if segment_manager.segment_type == constants.CHANNEL:
                related_obj.details["subscribers"] = item["subscribers"]
                related_obj.details["audited_videos"] = item["audited_videos"]
            to_create.append(related_obj)
        return to_create

    def _get_es_data(self, item_ids):
        response = self.es_connector.search_by_id(self.index_name, item_ids, constants.BRAND_SAFETY_SCORE_TYPE)
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

    def _clean(self, items):
        """
        Clean related segment model in case of script failurer
        :param item_ids:
        :return:
        """
        try:
            item_ids = [item[self.PK_NAME] for item in items]
        except KeyError:
            item_ids = items
        self.related_segment_model.objects.filter(related_id__in=item_ids).delete()

    def _channel_batch_generator(self, cursor_id=None):
        """
        Yields batch channel ids to audit
        :param cursor_id: Cursor position to start audit
        :return: list -> Youtube channel ids
        """
        params = {
            "fields": "channel_id,subscribers,title,category,thumbnail_image_url,language,likes,dislikes,views",
            "sort": "subscribers:desc",
            "size": self.CHANNEL_BATCH_LIMIT,
        }
        while self.batch_count <= self.CHANNEL_BATCH_COUNTER_LIMIT:
            params["channel_id__range"] = "{},".format(cursor_id or "")
            response = self.sdb_connector.get_channel_list(params, ignore_sources=True)
            channels = [item for item in response.get("items", []) if item["channel_id"] != cursor_id]
            if not channels:
                break
            yield channels
            cursor_id = channels[-1]
            # Update script tracker and cursors
            self.script_tracker = self.audit_provider.set_cursor(self.script_tracker, cursor_id, integer=False)
            self.cursor_id = self.script_tracker.cursor_id
            self.batch_count += 1

    def _video_batch_generator(self, cursor_id=None):
        """
        Yields batch channel ids to audit
        :param cursor_id: Cursor position to start audit
        :return: list -> Youtube channel ids
        """
        params = {
            "fields": "video_id,views,title",
            "sort": "views:desc",
            "size": self.VIDEO_BATCH_LIMIT,
        }
        while self.batch_count <= self.VIDEO_BATCH_LIMIT:
            params["video_id__range"] = "{},".format(cursor_id or "")
            response = self.sdb_connector.get_video_list(params, ignore_sources=True)
            videos = [item for item in response.get("items", []) if item["video_id"] != cursor_id]
            if not videos:
                break
            yield videos
            cursor_id = videos[-1]
            # Update script tracker and cursors
            self.script_tracker = self.audit_provider.set_cursor(self.script_tracker, cursor_id, integer=False)
            self.cursor_id = self.script_tracker.cursor_id
            self.batch_count += 1
