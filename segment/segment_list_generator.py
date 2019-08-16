from collections import defaultdict
import logging

from django.contrib.postgres.fields.jsonb import KeyTransform
from django.conf import settings
import uuid

from audit_tool.models import AuditCategory
from segment.models.persistent.constants import PersistentSegmentTitles
import brand_safety.constants as constants
from brand_safety.auditors.utils import AuditUtils
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.constants import SEGMENTS_UUID_FIELD
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent.constants import PersistentSegmentCategory
from segment.models.persistent.constants import PersistentSegmentTitles
from segment.utils import get_persistent_segment_connector_config_by_type


logger = logging.getLogger(__name__)


class SegmentListGenerator(object):
    SENTIMENT_THRESHOLD = 0.2
    MINIMUM_VIEWS = 10
    MINIMUM_SUBSCRIBERS = 10
    MINIMUM_BRAND_SAFETY_OVERALL_SCORE = 50
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS)
    WHITELIST_SIZE = 100000
    BLACKLIST_SIZE = 100000
    VIDEO_SORT_KEY = "views:desc"
    CHANNEL_SORT_KEY = {"subscribers": {"order": "desc"}}

    def __init__(self):
        self.video_manager = VideoManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))
        self.channel_manager = ChannelManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))

    def run(self):
        for category in AuditCategory.objects.all():
            # Generate new entry
            self._generate_channel_whitelist(category)

    def process_master(self):
        self._master_whitelist_videos()
        self._master_blacklist_videos()

        self._master_whitelist_channels()
        self._master_blacklist_channels()

    def _generate_channel_whitelist(self, category):
        # category_id = category.id
        # category_name = category.category_display
        # new_category_segment = PersistentSegmentChannel.objects.create(
        #     uuid=uuid.uuid4(),
        #     title=PersistentSegmentChannel.get_title(category_name, constants.WHITELIST),
        #     category=constants.WHITELIST,
        #     is_master=False
        # )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers").gte(self.MINIMUM_SUBSCRIBERS).get() \
                # & QueryBuilder().build().must().term().field("general_data.top_category").value(category_name).get() \
                # & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()
        search = self.channel_manager._search()
        search = search.query(query)
        search = search.sort("subscribers:desc")
        search = search.params(preserve_order=True)
        for doc in search.scan():
            item = doc
            pass


        # self.channel_manager.add_to_segment(query, new_category_segment.uuid)
        # # Clean old segments
        # self._clean_old_segments(category_id, new_category_segment.uuid)

    def _generate_video_whitelist(self, category):
        category_id = category.id
        category_name = category.category_display
        new_category_segment = PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentVideo.get_title(category_name, constants.WHITELIST),
            category=constants.WHITELIST,
            is_master=False
        )
        query = QueryBuilder().build().must().term().field(f"{Sections.GENERAL_DATA}.category").value(category).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").gte(self.SENTIMENT_THRESHOLD).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE)

        self.video_manager.add_to_segment(PersistentSegmentVideo, query)
        # Truncate list
        self._truncate_segment(self.video_manager, new_category_segment.uuid, self.WHITELIST_SIZE, self.VIDEO_SORT_KEY)

        self._clean_old_segments(self.video_manager, PersistentSegmentVideo, category_id, new_category_segment.uuid)

    def _clean_old_segments(self, es_manager, model, category_id, new_segment_uuid):
        # Delete old persistent segments with same audit category and delete from elasticsearch
        old_segments = model.objects.filter(audit_category_id=category_id).exclude(uuid=new_segment_uuid)
        old_uuids = old_segments.values_list("uuid", flat=True)
        # Delete from documents old segments
        for uuid in old_uuids:
            remove_query = QueryBuilder().build().must().term().field(SEGMENTS_UUID_FIELD).value(uuid).get()
            es_manager.remove_from_segment(remove_query, uuid)
        old_segments.delete()

    def _master_whitelist_videos(self):
        master_whitelist_videos = PersistentSegmentChannel.objects.get(
            title=PersistentSegmentTitles.VIDEOS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE)

        query = QueryBuilder.build().must().range().filed(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                & QueryBuilder.build().must().range().field(f"{Sections.STATS}.sentiment").lt(self.SENTIMENT_THRESHOLD).get() \
                & QueryBuilder.build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").lt(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE)

        self.video_manager.add_to_segment(query, master_whitelist_videos.uuid)
        self._truncate_segment(self.video_manager, master_whitelist_videos.uuid, self.WHITELIST_SIZE, self.VIDEO_SORT_KEY)

    def _master_blacklist_videos(self):
        master_blacklist_videos = PersistentSegmentChannel.objects.get(
            title=PersistentSegmentTitles.VIDEOS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE)

        query = QueryBuilder.build().must().range().filed(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                & QueryBuilder.build().must().range().field(f"{Sections.STATS}.sentiment").gte(self.SENTIMENT_THRESHOLD).get() \
                & QueryBuilder.build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").lt(
            self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE)

        self.video_manager.add_to_segment(query, master_blacklist_videos.uuid)
        self._truncate_segment(self.video_manager, master_blacklist_videos.uuid, self.BLACKLIST_SIZE, self.VIDEO_SORT_KEY)

    def _master_whitelist_channels(self):
        master_whitelist_channels = PersistentSegmentChannel.objects.get(title=PersistentSegmentTitles.CHANNELS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE)

        query = QueryBuilder.build().must().range().filed(f"{Sections.STATS}.subscribers").gte(self.MINIMUM_SUBSCRIBERS).get() \
                & QueryBuilder.build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE)

        self.channel_manager.add_to_segment(query, master_whitelist_channels.uuid)
        self._truncate_segment(self.channel_manager, master_whitelist_channels.uuid, self.WHITELIST_SIZE, self.CHANNEL_SORT_KEY)

    def _master_blacklist_channels(self):
        master_blacklist_channels = PersistentSegmentChannel.objects.get(title=PersistentSegmentTitles.CHANNELS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE)

        query = QueryBuilder.build().must().range().filed(f"{Sections.STATS}.subscribers").gte(self.MINIMUM_SUBSCRIBERS).get() \
                & QueryBuilder.build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").lt(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE)

        self.channel_manager.add_to_segment(query, master_blacklist_channels.uuid)
        self._truncate_segment(self.channel_manager, master_blacklist_channels.uuid, self.BLACKLIST_SIZE, self.CHANNEL_SORT_KEY)

    def _truncate_segment(self, es_manager, segment_uuid, size, sort_key):
        """
        Get all items in segment, sort, and get items after size to remove from segment
        :param segment:
        :param size:
        :return:
        """
        search_query = QueryBuilder.build().must().terms().field(SEGMENTS_UUID_FIELD).value(segment_uuid).get()
        docs_to_remove = es_manager.search(search_query, sort=sort_key, offset=size)

        ids_to_remove = [item.main.id for item in docs_to_remove]
        remove_query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value(ids_to_remove).get()
        es_manager.remove_from_segment(remove_query, segment_uuid)
