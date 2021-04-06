import logging
import uuid
from datetime import timedelta

from django.utils import timezone

import brand_safety.constants as constants
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentFileUpload
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import CATEGORY_THUMBNAIL_IMAGE_URLS
from segment.models.persistent.constants import PersistentSegmentTitles
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from segment.utils.generate_segment import generate_segment
from userprofile.constants import StaticPermissions
from utils.utils import chunks_generator

logger = logging.getLogger(__name__)

""" SegmentListGenerator - Used to generate brand suitable target lists for all AuditCategory values

Procedure:
    1. Retrieve all AuditCategory values
    2. Retrieve channel videos and blacklist data
    3. Instantiate BrandSafetyChannelAudit for each channel and run audit
    4. Instantiate BrandSafetyVideoAudit for each video and run audit
    5. Index results
"""


class SegmentListGenerator:
    MAX_API_CALL_RETRY = 15
    RETRY_SLEEP_COEFFICIENT = 2
    RETRY_ON_CONFLICT = 10000
    SENTIMENT_THRESHOLD = 80
    MINIMUM_VIEWS = 1000
    MINIMUM_SUBSCRIBERS = 1000
    WHITELIST_MINIMUM_BRAND_SAFETY_SCORE = 90
    BLACKLIST_BRAND_SAFETY_SCORE_THRESHOLD = 59
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)
    WHITELIST_SIZE = 100000
    BLACKLIST_SIZE = 100000

    CUSTOM_CHANNEL_SIZE = 20000
    CUSTOM_VIDEO_SIZE = 20000
    SEGMENT_BATCH_SIZE = 1000
    UPDATE_THRESHOLD = 7
    VIDEO_SORT_KEY = {"stats.views": {"order": "desc"}}
    CHANNEL_SORT_KEY = {"stats.subscribers": {"order": "desc"}}

    def __init__(self, generation_type):
        """
        :param generation_type: int -> Set configuration type (See run method)
        """
        self.type = generation_type
        self.processed_categories = set()

    def run(self):
        handlers = {
            0: self.generate_master_brand_suitable_target_lists,
        }
        handler = handlers[self.type]
        handler()

    def generate_master_brand_suitable_target_lists(self):
        """
        Generate brand suitable target lists for all master video/channel white/black lists
        """
        logger.debug("Processing master whitelists and blacklists")
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
        category_name = category.category_display_iab
        should_update = self.check_should_update(PersistentSegmentChannel, False, category_id, constants.WHITELIST)
        if should_update:
            # Generate new category segment
            new_category_segment = PersistentSegmentChannel.objects.create(
                uuid=uuid.uuid4(),
                title=PersistentSegmentChannel.get_title(category_name, constants.WHITELIST),
                category=constants.WHITELIST,
                is_master=False,
                audit_category_id=category_id,
                thumbnail_image_url=CATEGORY_THUMBNAIL_IMAGE_URLS.get(category_name,
                                                                      S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL)
            )
            try:
                query = QueryBuilder().build().must().term().field(f"{Sections.GENERAL_DATA}.iab_categories") \
                            .value(category_name).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers") \
                            .gte(self.MINIMUM_SUBSCRIBERS).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score") \
                            .gte(self.WHITELIST_MINIMUM_BRAND_SAFETY_SCORE).get()

                results = generate_segment(new_category_segment, query, self.WHITELIST_SIZE, add_uuid=True)
                self.persistent_segment_finalizer(new_category_segment, results)
                self._clean_old_segments(PersistentSegmentChannel, new_category_segment.uuid, category_id=category_id)
            # pylint: disable=broad-except
            except Exception:
                # pylint: enable=broad-except
                logger.exception("Error in _generate_channel_whitelist")
                new_category_segment.delete()

    def _generate_video_whitelist(self, category):
        category_id = category.id
        category_name = category.category_display_iab
        should_update = self.check_should_update(PersistentSegmentVideo, False, category_id, constants.WHITELIST)
        if should_update:
            # Generate new category segment
            new_category_segment = PersistentSegmentVideo.objects.create(
                uuid=uuid.uuid4(),
                title=PersistentSegmentVideo.get_title(category_name, constants.WHITELIST),
                category=constants.WHITELIST,
                is_master=False,
                audit_category_id=category_id,
                thumbnail_image_url=CATEGORY_THUMBNAIL_IMAGE_URLS.get(category_name,
                                                                      S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL)
            )
            try:
                query = QueryBuilder().build().must().term().field(f"{Sections.GENERAL_DATA}.iab_categories") \
                            .value(category_name).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.STATS}.views") \
                            .gte(self.MINIMUM_VIEWS).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment") \
                            .gte(self.SENTIMENT_THRESHOLD).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score") \
                            .gte(self.WHITELIST_MINIMUM_BRAND_SAFETY_SCORE).get()

                results = generate_segment(new_category_segment, query, self.WHITELIST_SIZE, add_uuid=True)
                self.persistent_segment_finalizer(new_category_segment, results)
                self._clean_old_segments(PersistentSegmentVideo, new_category_segment.uuid, category_id=category_id)
            # pylint: disable=broad-except
            except Exception:
                # pylint: enable=broad-except
                logger.exception("Error in _generate_video_whitelist")
                new_category_segment.delete()

    def _generate_master_video_whitelist(self):
        """
        Generate Master Video Whitelist
        :return:
        """
        should_update = self.check_should_update(PersistentSegmentVideo, True, None, constants.WHITELIST)
        if should_update:
            new_master_video_whitelist = PersistentSegmentVideo.objects.create(
                uuid=uuid.uuid4(),
                title=PersistentSegmentTitles.VIDEOS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE,
                category=constants.WHITELIST,
                is_master=True,
                audit_category_id=None
            )
            try:
                query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.views") \
                            .gte(self.MINIMUM_VIEWS).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment") \
                            .gte(self.SENTIMENT_THRESHOLD).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score") \
                            .gte(self.WHITELIST_MINIMUM_BRAND_SAFETY_SCORE).get()

                results = generate_segment(new_master_video_whitelist, query, self.WHITELIST_SIZE, add_uuid=True)
                self.persistent_segment_finalizer(new_master_video_whitelist, results)
                self._clean_old_segments(PersistentSegmentVideo, new_master_video_whitelist.uuid, is_master=True,
                                         master_list_type=constants.WHITELIST)
            # pylint: disable=broad-except
            except Exception:
                # pylint: enable=broad-except
                logger.exception("Error in _generate_master_video_whitelist")
                new_master_video_whitelist.delete()

    def _generate_master_video_blacklist(self):
        """
        Generate Master Video Blacklist
        :return:
        """
        should_update = self.check_should_update(PersistentSegmentVideo, True, None, constants.BLACKLIST)
        if should_update:
            new_master_video_blacklist = PersistentSegmentVideo.objects.create(
                uuid=uuid.uuid4(),
                title=PersistentSegmentTitles.VIDEOS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE,
                category=constants.BLACKLIST,
                is_master=True,
                audit_category_id=None
            )
            try:
                query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.views") \
                            .gte(self.MINIMUM_VIEWS).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment") \
                            .lt(self.SENTIMENT_THRESHOLD).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score") \
                            .lte(self.BLACKLIST_BRAND_SAFETY_SCORE_THRESHOLD).get()

                results = generate_segment(new_master_video_blacklist, query, self.BLACKLIST_SIZE, add_uuid=True)
                self.persistent_segment_finalizer(new_master_video_blacklist, results)
                self._clean_old_segments(PersistentSegmentVideo, new_master_video_blacklist.uuid, is_master=True,
                                         master_list_type=constants.BLACKLIST)
            # pylint: disable=broad-except
            except Exception:
                # pylint: enable=broad-except
                logger.exception("Error in _generate_master_video_blacklist")
                new_master_video_blacklist.delete()

    def _generate_master_channel_whitelist(self):
        """
        Generate Master Channel Whitelist
        :return:
        """
        should_update = self.check_should_update(PersistentSegmentChannel, True, None, constants.WHITELIST)
        if should_update:
            new_master_channel_whitelist = PersistentSegmentChannel.objects.create(
                uuid=uuid.uuid4(),
                title=PersistentSegmentTitles.CHANNELS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE,
                category=constants.WHITELIST,
                is_master=True,
                audit_category_id=None
            )
            try:
                query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers") \
                            .gte(self.MINIMUM_SUBSCRIBERS).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score") \
                            .gte(self.WHITELIST_MINIMUM_BRAND_SAFETY_SCORE).get()

                results = generate_segment(new_master_channel_whitelist, query, self.WHITELIST_SIZE, add_uuid=True)
                self.persistent_segment_finalizer(new_master_channel_whitelist, results)
                self._clean_old_segments(PersistentSegmentChannel, new_master_channel_whitelist.uuid, is_master=True,
                                         master_list_type=constants.WHITELIST)
            # pylint: disable=broad-except
            except Exception:
                # pylint: enable=broad-except
                logger.exception("Error in _generate_master_channel_whitelist")
                new_master_channel_whitelist.delete()

    def _generate_master_channel_blacklist(self):
        """
        Generate Master Channel Blacklist
        :return:
        """
        should_update = self.check_should_update(PersistentSegmentChannel, True, None, constants.BLACKLIST)
        if should_update:
            new_master_channel_blacklist = PersistentSegmentChannel.objects.create(
                uuid=uuid.uuid4(),
                title=PersistentSegmentTitles.CHANNELS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE,
                category=constants.BLACKLIST,
                is_master=True,
                audit_category_id=None
            )
            try:
                query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers") \
                            .gte(self.MINIMUM_SUBSCRIBERS).get() \
                        & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score") \
                            .lte(self.BLACKLIST_BRAND_SAFETY_SCORE_THRESHOLD).get()

                results = generate_segment(new_master_channel_blacklist, query, self.BLACKLIST_SIZE, add_uuid=True)
                self.persistent_segment_finalizer(new_master_channel_blacklist, results)
                self._clean_old_segments(PersistentSegmentChannel, new_master_channel_blacklist.uuid, is_master=True,
                                         master_list_type=constants.BLACKLIST)
            # pylint: disable=broad-except
            except Exception:
                # pylint: enable=broad-except
                logger.exception("Error in _generate_master_channel_blacklist")
                new_master_channel_blacklist.delete()

    def add_to_segment(self, segment, query=None, size=None):
        """
        Add segment uuid to Elasticsearch documents that match query
        """
        all_items = []
        if query is None:
            query = segment.get_segment_items_query()
        if size is None:
            if segment.owner.has_permission(StaticPermissions.BUILD__CTL_EXPORT_ADMIN):
                size = segment.config.ADMIN_LIST_SIZE
            else:
                size = segment.config.USER_LIST_SIZE
        search_with_params = segment.generate_search_with_params(query=query, sort=segment.config.SORT_KEY)
        for doc in search_with_params.scan():
            all_items.append(doc)
            if len(all_items) >= size:
                break

        for batch in chunks_generator([item.main.id for item in all_items], self.SEGMENT_BATCH_SIZE):
            segment.add_to_segment(doc_ids=batch)
        return all_items

    def _clean_old_segments(self, model, new_segment_uuid, category_id=None, is_master=False,
                            master_list_type=constants.WHITELIST):
        """
        Delete old category segments and clean documents with old segment uuid
        """
        # Delete old persistent segments with same audit category and delete from Elasticsearch
        if is_master:
            old_segments = model.objects.filter(category=master_list_type, is_master=True).exclude(
                uuid=new_segment_uuid)
        else:
            old_segments = model.objects.filter(audit_category_id=category_id).exclude(uuid=new_segment_uuid)
        # Delete old segment uuid's from documents
        for old_segment in old_segments:
            old_segment.delete()

    def persistent_segment_finalizer(self, segment, details):
        """
        Finalize operations for PersistentSegment objects (Brand Suitable Target lists)
        """
        segment.details = details["statistics"]
        segment.save()
        PersistentSegmentFileUpload.objects.create(segment_uuid=segment.uuid, filename=details["s3_key"],
                                                   created_at=timezone.now())
        logger.debug("Successfully generated export for brand suitable list: id: %s, title: %s",
                     segment.id, segment.title)

    def check_should_update(self, segment_model, is_master, category_id, segment_type):
        date_threshold = timezone.now() - timedelta(days=self.UPDATE_THRESHOLD)
        existing_items = segment_model.objects.filter(is_master=is_master, audit_category_id=category_id,
                                                      category=segment_type).order_by("-created_at")
        if not existing_items.exists() or existing_items.first().created_at <= date_threshold:
            return True
        return False
