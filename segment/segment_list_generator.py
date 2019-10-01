import logging

from django.utils import timezone
from elasticsearch_dsl.search import Q
import uuid

from administration.notifications import generate_html_email
from audit_tool.models import AuditCategory
import brand_safety.constants as constants
from brand_safety.auditors.utils import AuditUtils
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.constants import SEGMENTS_UUID_FIELD
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentFileUpload
from segment.models.persistent import PersistentSegmentVideo
from segment.models.persistent.constants import PersistentSegmentTitles
from segment.models.persistent.constants import CATEGORY_THUMBNAIL_IMAGE_URLS
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from segment.utils import retry_on_conflict
from segment.utils import generate_search_with_params
from utils.aws.s3_exporter import S3Exporter
from utils.aws.ses_emailer import SESEmailer


logger = logging.getLogger(__name__)


class SegmentListGenerator(S3Exporter):
    MAX_API_CALL_RETRY = 15
    RETRY_SLEEP_COEFFICIENT = 2
    SENTIMENT_THRESHOLD = 0.8
    MINIMUM_VIEWS = 1000
    MINIMUM_SUBSCRIBERS = 1000
    MINIMUM_BRAND_SAFETY_OVERALL_SCORE = 89
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY)
    WHITELIST_SIZE = 100000
    BLACKLIST_SIZE = 100000
    CUSTOM_CHANNEL_SIZE = 20000
    CUSTOM_VIDEO_SIZE = 20000
    SEGMENT_BATCH_SIZE = 2000
    VIDEO_SORT_KEY = {"stats.views": {"order": "desc"}}
    CHANNEL_SORT_KEY = {"stats.subscribers": {"order": "desc"}}

    def __init__(self, type):
        self.ses = SESEmailer()
        self.type = type
        self.video_manager = VideoManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))
        self.channel_manager = ChannelManager(sections=self.SECTIONS, upsert_sections=(Sections.SEGMENTS,))
        self.processed_categories = set()

    def run(self, segment=None):
        handlers = {
            "brand_suitability": self.generate_brand_suitable_lists,
            "custom": self.generate_custom_list
        }
        if segment:
            handler = handlers["custom"]
            handler(segment)
        else:
            handler = handlers["brand_suitability"]
            handler()

    def generate_brand_suitable_lists(self):
        for category in AuditCategory.objects.all():
            if category not in self.processed_categories:
                self._generate_channel_whitelist(category)
                self._generate_video_whitelist(category)
                self.processed_categories.add(category.category_display)

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
        logger.error(f"Processing channel: {category_name}")
        # Generate new category segment
        new_category_segment = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentChannel.get_title(category_name, constants.WHITELIST),
            category=constants.WHITELIST,
            is_master=False,
            audit_category_id=category_id,
            thumbnail_image_url=CATEGORY_THUMBNAIL_IMAGE_URLS.get(category_name, S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL)
        )
        query = QueryBuilder().build().must().term().field(f"{Sections.GENERAL_DATA}.top_category").value(category_name).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers").gte(self.MINIMUM_SUBSCRIBERS).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self.add_to_segment(new_category_segment, query=query, size=self.WHITELIST_SIZE)
        # self.add_to_segment(new_category_ segment.uuid, self.channel_manager, query, self.CHANNEL_SORT_KEY, self.WHITELIST_SIZE)
        # Clean old segments
        self._clean_old_segments(self.channel_manager, PersistentSegmentChannel, new_category_segment.uuid, category_id=category_id)
        # self.export_to_s3(new_category_segment)
        self.persistent_segment_finalizer(new_category_segment)

    def _generate_video_whitelist(self, category):
        category_id = category.id
        category_name = category.category_display
        logger.error(f"Processing video: {category_name}")
        # Generate new category segment
        new_category_segment = PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentVideo.get_title(category_name, constants.WHITELIST),
            category=constants.WHITELIST,
            is_master=False,
            audit_category_id=category_id,
            thumbnail_image_url=CATEGORY_THUMBNAIL_IMAGE_URLS.get(category_name, S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL)
        )
        query = QueryBuilder().build().must().term().field(f"{Sections.GENERAL_DATA}.category").value(category_name).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").gte(self.SENTIMENT_THRESHOLD).get() \
                    & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self.add_to_segment(new_category_segment, query=query, size=self.WHITELIST_SIZE)
        # self.add_to_segment(new_category_segment.uuid, self.video_manager, query, self.VIDEO_SORT_KEY, self.WHITELIST_SIZE)
        # Clean old segments
        self._clean_old_segments(self.video_manager, PersistentSegmentVideo, new_category_segment.uuid, category_id=category_id)
        # self.export_to_s3(new_category_segment)
        self.persistent_segment_finalizer(new_category_segment)

    def _generate_master_video_whitelist(self):
        """
        Generate Master Video Whitelist
        :return:
        """
        logger.error("Processing Master Video Whitelist")
        new_master_video_whitelist = PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentTitles.VIDEOS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE,
            category=constants.WHITELIST,
            is_master=True,
            audit_category_id=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").gte(self.SENTIMENT_THRESHOLD).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self.add_to_segment(new_master_video_whitelist, query=query, size=self.WHITELIST_SIZE)
        # self.add_to_segment(new_master_video_whitelist.uuid, self.video_manager, query, self.VIDEO_SORT_KEY, self.WHITELIST_SIZE)
        self._clean_old_segments(self.video_manager, PersistentSegmentVideo, new_master_video_whitelist.uuid, is_master=True, master_list_type=constants.WHITELIST)
        # self.export_to_s3(new_master_video_whitelist)
        self.persistent_segment_finalizer(new_master_video_whitelist)

    def _generate_master_video_blacklist(self):
        """
        Generate Master Video Blacklist
        :return:
        """
        logger.error("Processing Master Video Blacklist")
        new_master_video_blacklist = PersistentSegmentVideo.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentTitles.VIDEOS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE,
            category=constants.BLACKLIST,
            is_master=True,
            audit_category_id=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.views").gte(self.MINIMUM_VIEWS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment").lt(self.SENTIMENT_THRESHOLD).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").lt(
            self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self.add_to_segment(new_master_video_blacklist, query=query, size=self.BLACKLIST_SIZE)
        # self.add_to_segment(new_master_video_blacklist.uuid, self.video_manager, query, self.VIDEO_SORT_KEY, self.BLACKLIST_SIZE)
        self._clean_old_segments(self.video_manager, PersistentSegmentVideo, new_master_video_blacklist.uuid, is_master=True, master_list_type=constants.BLACKLIST)
        # self.export_to_s3(new_master_video_blacklist)
        self.persistent_segment_finalizer(new_master_video_blacklist)

    def _generate_master_channel_whitelist(self):
        """
        Generate Master Channel Whitelist
        :return:
        """
        logger.error("Processing Master Channel Whitelist")
        new_master_channel_whitelist = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentTitles.CHANNELS_BRAND_SUITABILITY_MASTER_WHITELIST_SEGMENT_TITLE,
            category=constants.WHITELIST,
            is_master=True,
            audit_category_id=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers").gte(self.MINIMUM_SUBSCRIBERS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").gte(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        self.add_to_segment(new_master_channel_whitelist, query=query, size=self.WHITELIST_SIZE)
        # self.add_to_segment(new_master_channel_whitelist.uuid, self.channel_manager, query, self.CHANNEL_SORT_KEY, self.WHITELIST_SIZE)
        self._clean_old_segments(self.channel_manager, PersistentSegmentChannel, new_master_channel_whitelist.uuid,
                                 is_master=True, master_list_type=constants.WHITELIST)
        # self.export_to_s3(new_master_channel_whitelist)
        self.persistent_segment_finalizer(new_master_channel_whitelist)

    def _generate_master_channel_blacklist(self):
        """
        Generate Master Channel Blacklist
        :return:
        """
        logger.error("Processing Master Channel Blacklist")
        new_master_channel_blacklist = PersistentSegmentChannel.objects.create(
            uuid=uuid.uuid4(),
            title=PersistentSegmentTitles.CHANNELS_BRAND_SUITABILITY_MASTER_BLACKLIST_SEGMENT_TITLE,
            category=constants.BLACKLIST,
            is_master=True,
            audit_category_id=None
        )
        query = QueryBuilder().build().must().range().field(f"{Sections.STATS}.subscribers").gte(self.MINIMUM_SUBSCRIBERS).get() \
                & QueryBuilder().build().must().range().field(f"{Sections.BRAND_SAFETY}.overall_score").lt(self.MINIMUM_BRAND_SAFETY_OVERALL_SCORE).get()

        # self.add_to_segment(new_master_channel_blacklist.uuid, self.channel_manager, query, self.CHANNEL_SORT_KEY, self.BLACKLIST_SIZE)
        self.add_to_segment(new_master_channel_blacklist, query=query, size=self.BLACKLIST_SIZE)
        self._clean_old_segments(self.channel_manager, PersistentSegmentChannel, new_master_channel_blacklist.uuid,
                                 is_master=True, master_list_type=constants.BLACKLIST)
        self.export_to_s3(new_master_channel_blacklist)
        self.persistent_segment_finalizer(new_master_channel_blacklist)

    def generate_custom_list(self, segment):
        export = segment.export
        owner = segment.owner

        es_manager = segment.get_es_manger()
        serializer = segment.get_serializer()

        segment.remove_all_from_segment()
        self.add_to_segment(segment, size=segment.LIST_SIZE)

        # Upload to s3
        # notification email
        self.custom_list_finalizer(segment)

    def add_to_segment(self, segment, query=None, size=None):
        """
        Add Elasticsearch items to segment uuid
        :param segment_uuid:
        :param es_manager:
        :param query:
        :param sort_key:
        :param limit:
        :return:
        """
        ids_to_add = []
        if query is None:
            query = segment.get_segment_items_query()
        if size is None:
            size = segment.LIST_SIZE
        search_with_params = segment.generate_search_with_params(query=query, sort=segment.SORT_KEY)
        es_manager = segment.get_es_manager()
        for doc in search_with_params.scan():
            ids_to_add.append(doc.main.id)
            if len(ids_to_add) >= size:
                break
        for batch in AuditUtils.batch(ids_to_add, self.SEGMENT_BATCH_SIZE):
            retry_on_conflict(es_manager.add_to_segment_by_ids, batch, segment.uuid, retry_amount=self.MAX_API_CALL_RETRY, sleep_coeff=self.RETRY_SLEEP_COEFFICIENT)

    # def add_to_segment(self, segment_uuid, es_manager, query, sort_key, size):
    #     """
    #     Add Elasticsearch items to segment uuid
    #     :param segment_uuid:
    #     :param es_manager:
    #     :param query:
    #     :param sort_key:
    #     :param limit:
    #     :return:
    #     """
    #     ids_to_add = []
    #     search_with_params = generate_search_with_params(es_manager, query, sort_key)
    #     for doc in search_with_params.scan():
    #         ids_to_add.append(doc.main.id)
    #         if len(ids_to_add) >= size:
    #             break
    #     for batch in AuditUtils.batch(ids_to_add, self.SEGMENT_BATCH_SIZE):
    #         retry_on_conflict(es_manager.add_to_segment_by_ids, batch, segment_uuid, retry_amount=self.MAX_API_CALL_RETRY, sleep_coeff=self.RETRY_SLEEP_COEFFICIENT)

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
            retry_on_conflict(es_manager.remove_from_segment, remove_query, uuid, retry_amount=self.MAX_API_CALL_RETRY, sleep_coeff=self.RETRY_SLEEP_COEFFICIENT)
        old_segments.delete()

    # def export(self, segment):
    #     now = timezone.now()
    #     s3_filename = segment.get_s3_key(datetime=now)
    #     logger.error("Collecting data for {}".format(s3_filename))
    #     self.export_to_s3(s3_filename)
    #     segment.details = segment.calculate_statistics()
    #     segment.save()
    #     now = timezone.now()
    #     PersistentSegmentFileUpload.objects.create(segment_uuid=segment.uuid, filename=s3_filename, created_at=now)
    #     logger.error("Saved {}".format(segment.get_s3_key(datetime=now)))

    def custom_list_finalizer(self, segment, updating=False):
        now = timezone.now()
        export = segment.export
        s3_key = segment.get_s3_key()
        download_url = self.generate_temporary_url(s3_key, time_limit=3600 * 24 * 7)
        if updating:
            export.updated_at = now
        else:
            export.completed_at = timezone.now()

        export.download_url = download_url
        export.save()

        subject = "Custom Target List: {}".format(segment.title)
        text_header = "Your Custom Target List {} is ready".format(segment.title)
        text_content = "<a href={download_url}>Click here to download</a>".format(download_url=download_url)
        html_email = generate_html_email(text_header, text_content)

        self.ses.send_email(segment.owner.email, subject, html_email)

    def persistent_segment_finalizer(self, segment):
        now = timezone.now()
        s3_filename = segment.get_s3_key(datetime=now)
        logger.error("Collecting data for {}".format(s3_filename))
        self.export_to_s3(s3_filename)
        segment.details = segment.calculate_statistics()
        segment.save()
        now = timezone.now()
        PersistentSegmentFileUpload.objects.create(segment_uuid=segment.uuid, filename=s3_filename, created_at=now)
        logger.error("Saved {}".format(segment.get_s3_key(datetime=now)))
