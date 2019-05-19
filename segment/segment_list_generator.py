from collections import defaultdict

from django.db.models import Q

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
    CHANNEL_BATCH_LIMIT = 100
    VIDEO_BATCH_LIMIT = 100
    MINIMUM_CHANNEL_SUBSCRIBERS = 1000
    MINIMUM_VIDEO_VIEWS = 1000
    CHANNEL_BATCH_COUNTER_LIMIT = 500
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

        self.batch_limit = None
        self.batch_count = 0
        self._set_config()

    def _set_config(self):
        """
        Set configuration depending on list generation type
        :return:
        """
        if self.list_generator_type == constants.CHANNEL:
            self.segment_model = PersistentSegmentChannel
            self.related_segment_model = PersistentSegmentRelatedChannel
            self.PK_NAME = "channel_id"
            self.evaluator = self._evaluate_channel
            self.index_name = constants.BRAND_SAFETY_CHANNEL_ES_INDEX
            self.SCORE_FAIL_THRESHOLD = self.CHANNEL_SCORE_FAIL_THRESHOLD
            self.batch_limit = self.CHANNEL_BATCH_LIMIT
            self.master_blacklist_segment, _ = PersistentSegmentChannel.objects.get_or_create(
                title=PersistentSegmentTitles.CHANNELS_BRAND_SAFETY_MASTER_BLACKLIST_SEGMENT_TITLE,
                category="blacklist")
            self.master_whitelist_segment, _ = PersistentSegmentChannel.objects.get_or_create(
                title=PersistentSegmentTitles.CHANNELS_BRAND_SAFETY_MASTER_WHITELIST_SEGMENT_TITLE,
                category="whitelist")
        elif self.list_generator_type == constants.VIDEO:
            self.segment_model = PersistentSegmentVideo
            self.related_segment_model = PersistentSegmentRelatedVideo
            self.PK_NAME = "video_id"
            self.evaluator = self._evaluate_video
            self.index_name = constants.BRAND_SAFETY_VIDEO_ES_INDEX
            self.batch_limit = self.VIDEO_BATCH_LIMIT
            self.master_blacklist_segment, _ = PersistentSegmentVideo.objects.get_or_create(
                title=PersistentSegmentTitles.VIDEOS_BRAND_SAFETY_MASTER_BLACKLIST_SEGMENT_TITLE, category="blacklist")
            self.master_whitelist_segment, _ = PersistentSegmentVideo.objects.get_or_create(
                title=PersistentSegmentTitles.VIDEOS_BRAND_SAFETY_MASTER_WHITELIST_SEGMENT_TITLE, category="whitelist")
        else:
            raise ValueError("Unsupported list generation type: {}".format(self.list_generator_type))

    def run(self):
        for batch in self.sdb_data_generator():
            self.process(batch)
        # Done here

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

    def process(self, sdb_items):
        item_ids = [item[self.PK_NAME] for item in sdb_items]
        es_items = self._get_es_data(item_ids)
        merged_items = self._merge_data(es_items, sdb_items)
        sorted_by_category_whitelist = self._sort_by_category(merged_items)
        for category, items in sorted_by_category_whitelist.items():
            segment_title = self._get_segment_title()
            try:
                whitelist_segment_manager = self.segment_model.objects.get(title=segment_title)
                to_create = self._instantiate_related_items(items, whitelist_segment_manager)
                self.related_segment_model.objects.bulk_create(to_create)
            except self.segment_model.DoesNotExist:
                print("Unable to get segment: {}".format(segment_title))
        self._save_master_results(merged_items)

    def _merge_data(self, es_data, sdb_data):
        """
        Update sdb data with es brand safety data
        :param es_data:
        :param sdb_data:
        :return: list
        """
        for doc in es_data:
            sdb_item = sdb_data.get(doc["_id"])
            if sdb_item is None:
                continue
            keywords = self._extract_keywords(doc["_source"])
            sdb_item[constants.BRAND_SAFETY_HITS] = keywords
            sdb_item["id"] = doc["_id"]
            sdb_item["overall_score"] = doc["_source"]["overall_score"]
            # Try to add videos_scored for channel objects
            try:
                sdb_item["audited_videos"] = doc["_source"]["videos_scored"]
            except KeyError:
                pass
        return sdb_data

    def _sort_by_category(self, items):
        """
        Sort objects by their category
        :param items: sdb data
        :return: list
        """
        items_by_category = defaultdict(list)
        for item in items.values():
            category = item.get("category")
            items_by_category[category].append(item)
        return items_by_category

    def _save_master_results(self, items, **kwargs):
        """
        Save Video and Channel audits based on their brand safety results to their respective persistent segments
        :param kwargs: Persistent segments to save to
        :return: None
        """
        # Persistent segments that store brand safety objects
        whitelist_segment = kwargs["whitelist_segment"]
        blacklist_segment = kwargs["blacklist_segment"]
        # Related segment model used to instantiate database objects
        related_segment_model = kwargs["related_segment_model"]
        # Sort audits by brand safety results
        brand_safety_pass, brand_safety_fail = self._sort_brand_safety(items)

        whitelist_to_create = self._instantiate_related_items(brand_safety_pass, whitelist_segment, related_segment_model)
        blacklist_to_create = self._instantiate_related_items(brand_safety_fail, blacklist_segment, related_segment_model)
        related_segment_model.objects.bulk_create(whitelist_to_create + blacklist_to_create)

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
                related_id=item["id"],
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
                    "overall_scoore": item["overall_score"]
                }
            )
            if segment_manager.segment_type == constants.CHANNEL:
                related_obj.details["subscribers"] = item["subscribers"]
                related_obj.details["audited_videos"] = item["audited_videos"]
            to_create.append(related_obj)
        return to_create

    def _get_es_data(self, item_ids):
        response = self.es_connector.search_by_id(self.index_name, item_ids)
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

    def _clean(self, item_ids, related_segment_model):
        """
        Clean related segment model in case of script failurer
        :param item_ids:
        :return:
        """
        related_segment_model.objects.filter(related_id__in=item_ids).delete()

    def _channel_batch_generator(self, cursor_id=None):
        """
        Yields batch channel ids to audit
        :param cursor_id: Cursor position to start audit
        :return: list -> Youtube channel ids
        """
        params = {
            "fields": "channel_id,subscribers,title",
            "sort": "subscribers:desc",
            "size": 10,
        }
        while self.batch_count <= self.CHANNEL_BATCH_COUNTER_LIMIT:
            params["channel_id__range"] = "{},".format(cursor_id or "")
            response = self.sdb_connector.get_channel_list(params, ignore_sources=True)
            channels = [item for item in response.get("items", []) if item["channel_id"] != cursor_id]
            if not channels:
                break
            yield channels
            cursor_id = channels[-1]
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
            "size": 10,
        }
        while self.batch_count <= self.VIDEO_BATCH_LIMIT:
            params["video_id__range"] = "{},".format(cursor_id or "")
            response = self.sdb_connector.get_video_list(params, ignore_sources=True)
            videos = [item for item in response.get("items", []) if item["video_id"] != cursor_id]
            if not videos:
                break
            yield videos
            cursor_id = videos[-1]
            self.batch_count += 1

    def _video_batch_generator(self):
        pass

    def process_items(self, es_data_generator, segment_model, related_segment_model, get_save_master_config):
        for es_data in es_data_generator:
            print("On: ", self.batch_count)

            item_type = segment_model.segment_type
            item_ids = [item["_id"] for item in es_data]
            if self.batch_count == 0 and self.cursor_id is not None:
                print("i am not from beginning and starting with 0")
                self._clean(item_ids, related_segment_model)
            # Get sdb data for related model object creation
            sdb_data = self._get_sdb_data(item_ids, item_type)
            # Update sdb data with es brand safety data
            updated_data = self._merge_data(es_data, sdb_data)

            sorted_by_category = self._sort_by_category(updated_data)
            for category, items in sorted_by_category.items():
                segment_title = self._get_segment_title(
                    item_type,
                    category,
                    PersistentSegmentCategory.WHITELIST
                )
                try:
                    segment_manager = segment_model.objects.get(title=segment_title)
                    to_create = self._instantiate_related_items(items, segment_manager, related_segment_model)
                    related_segment_model.objects.bulk_create(to_create)
                except segment_model.DoesNotExist:
                    print("Unable to get segment: {}".format(segment_title))

            self._save_master_results(updated_data, **get_save_master_config)
            self.batch_count += 1
            if self.batch_count >= 5:
                break