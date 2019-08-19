import csv
import logging

from django.utils import timezone

from brand_safety.audit_providers.standard_brand_safety_provider import StandardBrandSafetyProvider
import brand_safety.constants as constants
from segment.models.persistent import PersistentSegmentRelatedChannel
from segment.models.persistent import PersistentSegmentRelatedVideo
from segment.models.persistent import PersistentSegmentFileUpload
from segment.segment_list_generator import SegmentListGenerator
from segment.utils import get_persistent_segment_model_by_type

logger = logging.getLogger(__name__)


class CustomSegmentImporter(object):
    ALLOWED_SEGMENT_CATEGORIES = ["whitelist", "blacklist", "apex"]

    def __init__(self, *args, **kwargs):
        self.data_type = self._format(kwargs["data_type"])
        self.segment_category = self._format(kwargs["segment_category"])
        self.segment_thumbnail = self._format(kwargs["thumbnail"])
        self.segment_title = kwargs["title"]
        self.youtube_ids = self._read_csv(kwargs["path"])
        self.brand_safety_provider = StandardBrandSafetyProvider()
        self.list_generator = SegmentListGenerator(list_generator_type=self.data_type)

        self.config = self._get_config(self.data_type, self.segment_title)

    def run(self):
        provider = self.config["provider"]
        segment = self.config["segment"]
        to_create_ids = set(self.youtube_ids) - set(segment.related.filter(related_id__in=self.youtube_ids).values_list("related_id", flat=True))
        audits = provider(list(to_create_ids))

        to_create = self.list_generator.instantiate_related_items([audit.metadata for audit in audits], segment)
        self.config["related_model"].objects.bulk_create(to_create)
        self._finalize(segment)

    def _read_csv(self, path: str) -> list:
        """
        Import csv Youtube ids
        :param path: str
        :return: lis
        """
        with open(path, mode="r", encoding="utf-8-sig") as file:
            reader = csv.reader(file)
            ids = [row[0] for row in reader]
        return ids

    def _get_config(self, data_type: str, segment_title: str) -> dict:
        """
        Set importer config depending on data_type
        :param data_type: str -> channel, video
        :param segment_title: str -> Segment title
        :return: dict
        """
        if self.segment_category not in self.ALLOWED_SEGMENT_CATEGORIES:
            raise ValueError("Allowed segment categories: {}".format(self.ALLOWED_SEGMENT_CATEGORIES))

        segment_model = get_persistent_segment_model_by_type(data_type)
        options = {
            constants.VIDEO: {
                "provider": self.brand_safety_provider.manual_video_update,
                "related_model": PersistentSegmentRelatedVideo
            },
            constants.CHANNEL: {
                "provider": self.brand_safety_provider.manual_channel_update,
                "related_model": PersistentSegmentRelatedChannel
            },
        }
        config = options[data_type]
        config["segment"] = segment_model.objects.get_or_create(
            title=segment_title,
            category=self.segment_category,
            thumbnail_image_url=self.segment_thumbnail
        )[0]
        return config

    def _format(self, value: str) -> str:
        formatted = value.lower().strip()
        return formatted

    @staticmethod
    def _finalize(segment):
        segment.details = segment.calculate_statistics()
        segment.save()
        now = timezone.now()

        s3_filename = segment.get_s3_key(datetime=now)
        segment.export_to_s3(s3_filename)
        PersistentSegmentFileUpload.objects.create(segment_id=segment.id, filename=s3_filename, created_at=now)
        logger.error("Imported segment: {}".format(segment.title))
