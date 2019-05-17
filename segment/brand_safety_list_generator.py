from collections import defaultdict

from django.db.models import Q

from audit_tool.models import APIScriptTracker
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


class BrandSafetyListGenerator(object):
    CHANNEL_SCORE_FAIL_LIMIT = 0
    VIDEO_SCORE_FAIL_LIMIT = 89
    ES_SIZE = 10
    CHANNEL_SDB_PARAM_FIELDS = "channel_id,title,thumbnail_image_url,category,subscribers,likes,dislikes,views,language"
    VIDEO_SDB_PARAM_FIELDS = "video_id,title,tags,thumbnail_image_url,category,likes,dislikes,views,language,transcript"

    def __init__(self, *_, **kwargs):
        self.channel_script_tracker = kwargs["channel_api_tracker"]
        self.video_script_tracker = kwargs["video_api_tracker"]
        self.channel_cursor_id = self.channel_script_tracker.cursor_id
        self.video_cursor_id = self.video_script_tracker.cursor_id
        self.sdb_connector = SingleDatabaseApiConnector()
        # self._set_master_segments()
        self.es_connector = ElasticSearchConnector()
        self.audit_provider = AuditProvider()
        self.dups = {}

    def run(self):
        count = 0
        print('running')
        channel_generator = self._es_generator(
            "channel_id", self.CHANNEL_SCORE_FAIL_LIMIT, constants.BRAND_SAFETY_CHANNEL_ES_INDEX, last_id=self.channel_cursor_id
        )
        for channel_batch in channel_generator:
            self.process(channel_batch, PersistentSegmentChannel, PersistentSegmentRelatedChannel)
            count +=1
            if count >= 20:
                break

        print("dups", self.dups)
        # for video_batch in self._es_generator(last_id=self.video_script_tracker):
        #     pass

    def process(self, es_data, segment_model, related_segment_model):
        item_type = segment_model.segment_type
        item_ids = [item["_id"] for item in es_data]
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
        audits_by_category = defaultdict(list)
        for item in items.values():
            category = item.get("category")
            audits_by_category[category].append(item)
        return audits_by_category

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

    def _instantiate_related_items(self, items, segment_manager, related_segment_model):
        """
        Instantiate related objects
        :param items: sdb data merged with es data
        :param segment_manager: target persistent segment
        :param related_segment_model: related segment model
        :return: list
        """
        to_create = []
        for item in items:
            related_obj = related_segment_model(
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
                    "bad_words": item[constants.BRAND_SAFETY_HITS]
                }
            )
            if segment_manager.segment_type == constants.CHANNEL:
                related_obj.details["subscribers"] = item["subscribers"]
                related_obj.details["audited_videos"] = item["audited_videos"]
            to_create.append(related_obj)
        return to_create

    def _es_generator(self, id_field, score_limit, index, last_id=None):
        """
        Generator for es data
        :param id_field:
        :param score_limit:
        :param index:
        :param last_id:
        :return:
        """
        last_id = last_id or ""
        while True:
            body = {
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {"overall_score": {"gte": score_limit}}},
                            {"range": {id_field: {"gte": last_id}}}
                        ]
                    }
                }
            }
            response = self.es_connector.search(index=index, sort=id_field, size=self.ES_SIZE, body=body)
            for item in response["hits"]["hits"]:
                if item["_id"] is last_id:
                    print("FOUND", last_id)
            items = [item for item in response["hits"]["hits"] if item["_id"] != last_id]
            print("Items retrieved: {}".format(len(items)))

            for item in items:
                _id = item["_id"]
                if self.dups.get(_id) is None:
                    self.dups[_id] = True

            yield items
            last_id = items[-1].get("_id")

            if not items:
                break

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

