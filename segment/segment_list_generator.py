from collections import defaultdict
import logging

from django.contrib.postgres.fields.jsonb import KeyTransform
from django.conf import settings

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
    MINIMUM_VIEWS = 1000
    MINIMUM_SUBSCRIBERS = 1000
    MINIMUM_BRAND_SAFETY_OVERALL_SCORE = 89
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS)
    WHITELIST_SIZE = 100000
    BLACKLIST_SIZE = 100000
    VIDEO_SORT_KEY = "views"
    CHANNEL_SORT_KEY = "subscribers"


    def __init__(self):
        self.video_manager = VideoManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))
        self.channel_manager = VideoManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))


    def run(self):
        self.process_categories()
        self.process_master()

    def process_categories(self):
        for segment in PersistentSegmentChannel.objects.all():
            self._whitelist_category_channels(segment.category, segment.uuid)

        for segment in PersistentSegmentVideo.objects.all():
            self._whitelist_category_videos(segment.category, segment.uuid)

    def process_master(self):
        self._master_whitelist_videos()
        self._master_blacklist_videos()

        self._master_whitelist_channels()
        self._master_blacklist_channels()

    def _whitelist_category_videos(self, category, segment_uuid):
        query = QueryBuilder.build().must().term().field(f"{Sections.GENERAL_DATA}.category").value(category).get() \
                & QueryBuilder.build().must().range().filed(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                & QueryBuilder.build().must().range().field(f"{Sections.STATS}.sentiment").gte(self.SENTIMENT_THRESHOLD).get() \
                & QueryBuilder.build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE)

        self.video_manager.add_to_segment(query, segment_uuid)
        self._truncate_segment(self.video_manager, segment_uuid, self.WHITELIST_SIZE, self.VIDEO_SORT_KEY)

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

    def _whitelist_category_channels(self, category, segment_uuid):
        query = QueryBuilder.build().must().term().field(f"{Sections.GENERAL_DATA}.top_category").value(category).get() \
                & QueryBuilder.build().must().range().filed(f"{Sections.STATS}.subscribers").lt(self.MINIMUM_SUBSCRIBERS).get() \
                & QueryBuilder.build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").lt(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE)

        self.channel_manager.add_to_segment(query, segment_uuid)
        self._truncate_segment(self.channel_manager, segment_uuid, self.WHITELIST_SIZE, self.CHANNEL_SORT_KEY)

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
