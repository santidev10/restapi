import logging
import time

import uuid

from audit_tool.models import AuditCategory
import brand_safety.constants as constants
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.constants import SEGMENTS_UUID_FIELD
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentTitles


logger = logging.getLogger(__name__)


class SegmentListGenerator(object):
    MAX_API_CALL_RETRY = 20
    RETRY_SLEEP_COEFFICIENT = 2
    SENTIMENT_THRESHOLD = 0.8
    MINIMUM_VIEWS = 1000
    MINIMUM_SUBSCRIBERS = 1000
    MINIMUM_BRAND_SAFETY_OVERALL_SCORE = 89
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)
    WHITELIST_SIZE = 100000
    BLACKLIST_SIZE = 100000
    SEGMENT_BATCH_SIZE = 500
    VIDEO_SORT_KEY = {"stats.views": {"order": "desc"}}
    CHANNEL_SORT_KEY = {"stats.subscribers": {"order": "desc"}}

    def __init__(self):
        self.video_manager = VideoManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))
        self.channel_manager = ChannelManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))

    def run(self):
        for category in AuditCategory.objects.all():
            self._generate_channel_whitelist(category)
            self._generate_video_whitelist(category)

        self._generate_master_channel_blacklist()
        self._generate_master_channel_whitelist()

        self._generate_master_video_blacklist()
        self._generate_master_video_whitelist()

    def _generate_channel_whitelist(self, category):
        """
        Generate Channel Category Whitelist
        :param category:
        :return:
        """
        category_id = category.id
        category_name = category.category_display
        # Generate new category segment
        new_category_segment = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentChannel.get_title(category_name, constants.WHITELIST),
            category=constants.WHITELIST,
            is_master=False,
            audit_category_id=category_id
        )
        query = QueryBuilder().build().must().term().field(f"{Sections.GENERAL_DATA}.top_category").value(category_name).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers").gte(self.MINIMUM_SUBSCRIBERS).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self._retry_on_conflict(self._add_to_segment, new_category_segment.uuid, self.channel_manager, query, self.CHANNEL_SORT_KEY, self.WHITELIST_SIZE)
        # Clean old segments
        self._clean_old_segments(self.channel_manager, PersistentSegmentChannel, new_category_segment.uuid, category_id=category_id)

    def _generate_video_whitelist(self, category):
        category_id = category.id
        category_name = category.category_display
        # Generate new category segment
        new_category_segment = PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentVideo.get_title(category_name, constants.WHITELIST),
            category=constants.WHITELIST,
            is_master=False,
            audit_category_id=category_id
        )
        query = QueryBuilder().build().must().term().field(f"{Sections.GENERAL_DATA}.category").value(category_name).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").gte(self.SENTIMENT_THRESHOLD).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self._retry_on_conflict(self._add_to_segment, new_category_segment.uuid, self.video_manager, query, self.VIDEO_SORT_KEY, self.WHITELIST_SIZE)
        # Clean old segments
        self._clean_old_segments(self.video_manager, PersistentSegmentVideo, new_category_segment.uuid, category_id=category_id)

    def _generate_master_video_whitelist(self):
        """
        Generate Master Video Whitelist
        :return:
        """
        new_master_video_whitelist = PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentTitles.VIDEOS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE,
            category=constants.WHITELIST,
            is_master=True,
            audit_category=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").gte(self.SENTIMENT_THRESHOLD).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self._add_to_segment(new_master_video_whitelist.uuid, self.video_manager, query, self.VIDEO_SORT_KEY, self.WHITELIST_SIZE)
        self._clean_old_segments(self.video_manager, PersistentSegmentVideo, new_master_video_whitelist.uuid, is_master=True, master_list_type=constants.WHITELIST)

    def _generate_master_video_blacklist(self):
        """
        Generate Master Video Blacklist
        :return:
        """
        new_master_video_blacklist = PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentTitles.VIDEOS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE,
            category=constants.BLACKLIST,
            is_master=True,
            audit_category=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").lt(self.SENTIMENT_THRESHOLD).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").lt(
            self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self._add_to_segment(new_master_video_blacklist.uuid, self.video_manager, query, self.VIDEO_SORT_KEY,
                             self.BLACKLIST_SIZE)
        self._clean_old_segments(self.video_manager, PersistentSegmentVideo, new_master_video_blacklist.uuid, is_master=True, master_list_type=constants.BLACKLIST)

    def _generate_master_channel_whitelist(self):
        """
        Generate Master Channel Whitelist
        :return:
        """
        new_master_channel_whitelist = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentTitles.CHANNELS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE,
            category=constants.WHITELIST,
            is_master=True,
            audit_category=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers").gte(self.MINIMUM_SUBSCRIBERS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self._add_to_segment(new_master_channel_whitelist.uuid, self.channel_manager, query, self.CHANNEL_SORT_KEY,
                             self.WHITELIST_SIZE)
        self._clean_old_segments(self.channel_manager, PersistentSegmentChannel, new_master_channel_whitelist.uuid,
                                 is_master=True, master_list_type=constants.WHITELIST)

    def _generate_master_channel_blacklist(self):
        """
        Generate Master Channel Blacklist
        :return:
        """
        new_master_channel_blacklist = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentTitles.CHANNELS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE,
            category=constants.BLACKLIST,
            is_master=True,
            audit_category=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers").gte(self.MINIMUM_SUBSCRIBERS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").lt(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self._add_to_segment(new_master_channel_blacklist.uuid, self.channel_manager, query, self.CHANNEL_SORT_KEY, self.BLACKLIST_SIZE)
        self._clean_old_segments(self.channel_manager, PersistentSegmentChannel, new_master_channel_blacklist.uuid,
                                 is_master=True, master_list_type=constants.BLACKLIST)

    def _add_to_segment(self, segment_uuid, es_manager, query, sort_key, size):
        """
        Add Elasticsearch items to segment uuid
        :param segment_uuid:
        :param es_manager:
        :param query:
        :param sort_key:
        :param limit:
        :return:
        """
        item_counter = 0
        ids_to_add = []
        search_with_params = self.generate_search_with_params(es_manager, query, sort_key)

        for doc in search_with_params.scan():
            ids_to_add.append(doc.main.id)
            item_counter += 1

            if len(ids_to_add) >= self.SEGMENT_BATCH_SIZE:
                es_manager.add_to_segment_by_ids(ids_to_add, segment_uuid)
                ids_to_add.clear()

            if item_counter >= size:
                es_manager.add_to_segment_by_ids(ids_to_add, segment_uuid)
                break
    
    @staticmethod
    def generate_search_with_params(manager, query, sort=None):
        """
        Generate scan query with sorting
        :param manager:
        :param query:
        :param sort:
        :return:
        """
        search = manager._search()
        search = search.query(query)
        if sort:
            search = search.sort(sort)
        search = search.params(preserve_order=True)
        return search

    def _clean_old_segments(self, es_manager, model, new_segment_uuid, category_id=None, is_master=False, master_list_type=constants.WHITELIST):
        """
        Delete old category segments and clean documents with old segment uuid
        :param es_manager:
        :param model:
        :param category_id:
        :param new_segment_uuid:
        :return:
        """
        # Delete old persistent segments with same audit category and delete from Elasticsearch
        if is_master:
            old_segments = model.objects.filter(category=master_list_type, is_master=True).exclude(uuid=new_segment_uuid)
        else:
            old_segments = model.objects.filter(audit_category_id=category_id).exclude(uuid=new_segment_uuid)

        old_uuids = old_segments.values_list("uuid", flat=True)
        # Delete old segment uuid's from documents
        for uuid in old_uuids:
            remove_query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(uuid).get()
            self._retry_on_conflict(es_manager.remove_from_segment, remove_query, uuid)
        old_segments.delete()

    def _retry_on_conflict(self, method, *args, **kwargs):
        """
        Retry on Document Conflicts
        """
        tries_count = 0
        try:
            while tries_count <= self.MAX_API_CALL_RETRY:
                try:
                    result = method(*args, **kwargs)
                except Exception as err:
                    if "ConflictError(409" in str(err):
                        tries_count += 1
                        if tries_count <= self.MAX_API_CALL_RETRY:
                            sleep_seconds_count = self.MAX_API_CALL_RETRY \
                                                  ** self.RETRY_SLEEP_COEFFICIENT
                            time.sleep(sleep_seconds_count)
                    else:
                        raise err
                else:
                    return result
        except Exception as e:
            logger.error("Unable to complete request", e)
