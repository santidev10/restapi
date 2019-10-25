import csv
import logging
import time

from django.db import IntegrityError
import uuid

from audit_tool.models import AuditCategory
import brand_safety.constants as constants
from es_components.managers import ChannelManager
from es_components.constants import Sections
from es_components.managers import VideoManager
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from segment.utils import get_persistent_segment_model_by_type
from segment.segment_list_generator import SegmentListGenerator

logger = logging.getLogger(__name__)


class PersistentSegmentImporter(object):
    ALLOWED_DATA_TYPES = (constants.CHANNEL, constants.VIDEO)
    ALLOWED_SEGMENT_CATEGORIES = ("whitelist", "blacklist", "apex")
    EXPORT_ATTEMPT_LIMIT = 15
    EXPORT_ATTEMPT_SLEEP = 5

    def __init__(self, *args, **kwargs):
        # None values are set in _setup
        self.segment = None
        self.youtube_ids = None
        self.es_manager = None
        self.csv_path = kwargs["path"]
        self.data_type = self.format(kwargs["data_type"])
        self.segment_category = self.format(kwargs["segment_type"])
        self.segment_thumbnail = kwargs["thumbnail"] or S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
        self.segment_title = kwargs["title"]
        self.audit_category_id = kwargs["audit_category"]

        self._setup()

    def _setup(self):
        """
        Create new segment and set youtube ids to add
        :return: None
        """
        if self.segment_category not in self.ALLOWED_SEGMENT_CATEGORIES:
            raise ValueError("Allowed segment categories: {}".format(self.ALLOWED_SEGMENT_CATEGORIES))

        # If an audit category is provided, try retrieving it
        if self.audit_category_id:
            self.audit_category_id = int(self.audit_category_id)
            if not AuditCategory.objects.filter(id=self.audit_category_id).exists():
                raise ValueError(f"Audit category does not exist: {self.audit_category_id}")

        # Set es_manager
        if self.data_type == constants.VIDEO:
            self.es_manager = VideoManager(upsert_sections=(Sections.SEGMENTS,))
            segment_max_size = SegmentListGenerator.CUSTOM_VIDEO_SIZE
        elif self.data_type == constants.CHANNEL:
            self.es_manager = ChannelManager(upsert_sections=(Sections.SEGMENTS,))
            segment_max_size = SegmentListGenerator.CUSTOM_CHANNEL_SIZE
        else:
            raise ValueError("Allowed data types: {}".format(self.ALLOWED_DATA_TYPES))

        self.youtube_ids = self._read_csv(self.csv_path)
        if len(self.youtube_ids) > segment_max_size:
            raise ValueError(f"Max allowed items for {self.data_type} segments: {segment_max_size} items.")

        if any("/" in _id for _id in self.youtube_ids):
            raise ValueError("Youtube ids must be id values.")

        segment_model = get_persistent_segment_model_by_type(self.data_type)
        try:
            self.segment = segment_model.objects.create(
                title=self.segment_title,
                category=self.segment_category,
                thumbnail_image_url=self.segment_thumbnail,
                audit_category_id=self.audit_category_id,
                uuid=uuid.uuid4(),
                is_master=False,
            )
        except IntegrityError as e:
            raise ValueError(f"Unable to create segment: {e}")

    def run(self):
        # Add item to segment
        self.segment.add_to_segment(doc_ids=self.youtube_ids)

        exported = False
        # Wait until all items have been added to segment
        attempts = 1
        while attempts <= self.EXPORT_ATTEMPT_LIMIT:
            logger.info(f"On export attempt {attempts} of {self.EXPORT_ATTEMPT_SLEEP}")
            query = self.segment.get_segment_items_query()
            es_manager = self.segment.get_es_manager()
            segment_items_count = es_manager.search(query, limit=0).execute().hits.total

            if segment_items_count == len(self.youtube_ids):
                # Calculate statistics and export
                self.segment.details = self.segment.calculate_statistics()
                self.segment.export_file()
                exported = True
                break
            else:
                time.sleep(attempts ** self.EXPORT_ATTEMPT_SLEEP)
                attempts += 1

        # If all items are not added to segment after retries, manually verify if segment is ready for export
        if not exported:
            raise Exception(f"Unable to add all items to segment with uuid: {self.segment.uuid}. Export failed.")
        logger.info(f"Finished segment import with uuid: {self.segment.uuid}")

    def _read_csv(self, path: str) -> list:
        """
        Import csv Youtube ids
            Values should be ids, not urls
        :param path: str
        :return: list
        """
        with open(path, mode="r", encoding="utf-8-sig") as file:
            reader = csv.reader(file)
            ids = [row[0] for row in reader]
        return ids

    @staticmethod
    def format(value: str) -> str:
        formatted = value.lower().strip()
        return formatted
