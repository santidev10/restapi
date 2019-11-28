import logging
import uuid
from datetime import timedelta

from django.db.models import Q
from django.utils import timezone
from django.conf import settings

import brand_safety.constants as constants
from administration.notifications import send_html_email
from audit_tool.models import AuditCategory
from brand_safety.auditors.utils import AuditUtils
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from segment.models import CustomSegmentFileUpload
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import CATEGORY_THUMBNAIL_IMAGE_URLS
from segment.models.persistent.constants import PersistentSegmentTitles
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL

logger = logging.getLogger(__name__)


class SegmentListGenerator(object):
    MAX_API_CALL_RETRY = 15
    RETRY_SLEEP_COEFFICIENT = 2
    RETRY_ON_CONFLICT = 10000
    SENTIMENT_THRESHOLD = 0.8
    MINIMUM_VIEWS = 1000
    MINIMUM_SUBSCRIBERS = 1000
    MINIMUM_BRAND_SAFETY_OVERALL_SCORE = 89
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)
    WHITELIST_SIZE = 100000
    BLACKLIST_SIZE = 100000

    CUSTOM_CHANNEL_SIZE = 20000
    CUSTOM_VIDEO_SIZE = 20000
    SEGMENT_BATCH_SIZE = 1000
    UPDATE_THRESHOLD = 7
    VIDEO_SORT_KEY = {"stats.views": {"order": "desc"}}
    CHANNEL_SORT_KEY = {"stats.subscribers": {"order": "desc"}}

    def __init__(self, type):
        """
        :param type: int -> Set configuration type (See run method)
        """
        self.type = type
        self.processed_categories = set()

    def run(self):
        handlers = {
            0: self.generate_brand_suitable_lists,
            1: self.generate_custom_lists,
            2: self.update_custom_lists
        }
        handler = handlers[self.type]
        handler()

    def generate_brand_suitable_lists(self):
        """
        Generate brand suitable target lists with Youtube categories
        """
        for category in AuditCategory.objects.all():
            logger.debug(f"Processing audit category: id: {category.id}, name: {category.category_display}")
            if category.category_display not in self.processed_categories:
                self._generate_channel_whitelist(category)
                self._generate_video_whitelist(category)
                self.processed_categories.add(category.category_display)

        logger.debug("Processing master whitelists and blacklists")
        self._generate_master_channel_blacklist()
        self._generate_master_channel_whitelist()

        self._generate_master_video_blacklist()
        self._generate_master_video_whitelist()

    def generate_custom_lists(self):
        """
        Generate new custom target lists
        """
        dequeue_export = CustomSegmentFileUpload.objects.filter(completed_at=None).first()
        while dequeue_export:
            segment = dequeue_export.segment
            segment.remove_all_from_segment()

            all_items = self.add_to_segment(segment, query=dequeue_export.query_obj, size=segment.LIST_SIZE)

            self.custom_list_finalizer(segment, dequeue_export, all_items)
            dequeue_export = CustomSegmentFileUpload.objects.filter(completed_at=None).first()

    def update_custom_lists(self):
        """
        Update existing target lists older than threshold
        """
        threshold = timezone.now() - timedelta(days=self.UPDATE_THRESHOLD)
        to_update = CustomSegmentFileUpload.objects.filter(
            (Q(updated_at__isnull=True) & Q(created_at__lte=threshold)) | Q(updated_at__lte=threshold)
        )
        for export in to_update:
            if export.segment.owner is None:
                continue
            segment = export.segment
            segment.remove_all_from_segment()
            all_items = self.add_to_segment(segment, query=export.query_obj, size=segment.LIST_SIZE)
            self.custom_list_finalizer(segment, export, all_items, updating=True)

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
            audit_category_id=category_id,
            thumbnail_image_url=CATEGORY_THUMBNAIL_IMAGE_URLS.get(category_name,
                                                                  S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL)
        )
        query = QueryBuilder().build().must().term().field(f"{Sections.GENERAL_DATA}.top_category").value(
            category_name).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers").gte(
            self.MINIMUM_SUBSCRIBERS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(
            self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        all_items = self.add_to_segment(new_category_segment, query=query, size=self.WHITELIST_SIZE)
        # Clean old segments
        self._clean_old_segments(PersistentSegmentChannel, new_category_segment.uuid, category_id=category_id)
        self.persistent_segment_finalizer(new_category_segment, all_items)

    def _generate_video_whitelist(self, category):
        category_id = category.id
        category_name = category.category_display
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
        query = QueryBuilder().build().must().term().field(f"{Sections.GENERAL_DATA}.category").value(
            category_name).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").gte(
            self.SENTIMENT_THRESHOLD).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(
            self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        all_items = self.add_to_segment(new_category_segment, query=query, size=self.WHITELIST_SIZE)
        self._clean_old_segments(PersistentSegmentVideo, new_category_segment.uuid, category_id=category_id)
        self.persistent_segment_finalizer(new_category_segment, all_items)

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
            audit_category_id=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").gte(
            self.SENTIMENT_THRESHOLD).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(
            self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        all_items = self.add_to_segment(new_master_video_whitelist, query=query, size=self.WHITELIST_SIZE)
        self._clean_old_segments(PersistentSegmentVideo, new_master_video_whitelist.uuid, is_master=True,
                                 master_list_type=constants.WHITELIST)
        self.persistent_segment_finalizer(new_master_video_whitelist, all_items)

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
            audit_category_id=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").lt(
            self.SENTIMENT_THRESHOLD).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").lt(
            self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        all_items = self.add_to_segment(new_master_video_blacklist, query=query, size=self.BLACKLIST_SIZE)
        self._clean_old_segments(PersistentSegmentVideo, new_master_video_blacklist.uuid, is_master=True,
                                 master_list_type=constants.BLACKLIST)
        self.persistent_segment_finalizer(new_master_video_blacklist, all_items)

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
            audit_category_id=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers").gte(
            self.MINIMUM_SUBSCRIBERS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(
            self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        all_items = self.add_to_segment(new_master_channel_whitelist, query=query, size=self.WHITELIST_SIZE)
        self._clean_old_segments(PersistentSegmentChannel, new_master_channel_whitelist.uuid, is_master=True,
                                 master_list_type=constants.WHITELIST)
        self.persistent_segment_finalizer(new_master_channel_whitelist, all_items)

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
            audit_category_id=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers").gte(
            self.MINIMUM_SUBSCRIBERS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").lt(
            self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        all_items = self.add_to_segment(new_master_channel_blacklist, query=query, size=self.BLACKLIST_SIZE)
        self._clean_old_segments(PersistentSegmentChannel, new_master_channel_blacklist.uuid, is_master=True,
                                 master_list_type=constants.BLACKLIST)
        self.persistent_segment_finalizer(new_master_channel_blacklist, all_items)

    def add_to_segment(self, segment, query=None, size=None):
        """
        Add segment uuid to Elasticsearch documents that match query
        """
        all_items = []
        if query is None:
            query = segment.get_segment_items_query()
        if size is None:
            size = segment.LIST_SIZE
        search_with_params = segment.generate_search_with_params(query=query, sort=segment.SORT_KEY)
        for doc in search_with_params.scan():
            all_items.append(doc)
            if len(all_items) >= size:
                break

        for batch in AuditUtils.batch([item.main.id for item in all_items], self.SEGMENT_BATCH_SIZE):
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

    def custom_list_finalizer(self, segment, export, all_items, updating=False):
        """
        Finalize operations for CustomSegment objects (Custom Target Lists)
        """
        segment.statistics = segment.calculate_statistics(items=all_items)
        segment.export_file(updating=updating, queryset=all_items)
        segment.save()
        export.refresh_from_db()
        if updating is False:
            subject = "Custom Target List: {}".format(segment.title)
            text_header = "Your Custom Target List {} is ready".format(segment.title)
            text_content = "<a href={download_url}>Click here to download</a>".format(download_url=export.download_url)
            send_html_email(
                subject=subject,
                to=segment.owner.email,
                text_header=text_header,
                text_content=text_content,
                from_email=settings.EXPORTS_EMAIL_ADDRESS
            )
            message = "updated" if updating else "generated"
        logger.debug(f"Successfully {message} export for custom list: id: {segment.id}, title: {segment.title}")

    def persistent_segment_finalizer(self, segment, all_items):
        """
        Finalize operations for PersistentSegment objects (Brand Suitable Target lists)
        """
        segment.details = segment.calculate_statistics(items=all_items)
        segment.export_file(queryset=all_items)
        segment.save()
        logger.debug(
            f"Successfully generated export for brand suitable list: id: {segment.id}, title: {segment.title}")
