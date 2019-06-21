from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import DateTimeField
from django.db.models import OneToOneField
from django.db.models import Model
from django.db.models import TextField

from segment.models.custom_segment import CustomSegment


class CustomSegmentFileUpload(Model):
    BASE_COLUMNS = ["url", "title", "language", "youtube_category", "overall_score"]
    CHANNEL_COLUMNS = BASE_COLUMNS + ["subscribers"]
    VIDEO_COLUMNS = BASE_COLUMNS + ["views"]

    completed_at = DateTimeField(null=True, default=None, db_index=True)
    created_at = DateTimeField(auto_now_add=True, db_index=True)
    download_url = TextField(null=True)
    segment = OneToOneField(CustomSegment, related_name="export")
    query = JSONField()
    updated_at = DateTimeField(null=True, db_index=True)

    @staticmethod
    def enqueue(*_, **kwargs):
        """
        Interface to create new export entry
        :param _:
        :param kwargs: Model field values
        :return: CustomSegmentFileUpload
        """
        enqueue_item = CustomSegmentFileUpload.objects.create(**kwargs)
        return enqueue_item

    @staticmethod
    def dequeue():
        """
        Interface to return first export entry to be processed
        :return: CustomSegmentFileUpload
        """
        dequeue_item = CustomSegmentFileUpload.objects.filter(completed_at=None).order_by("created_at").first()
        if not dequeue_item:
            raise CustomSegmentFileUploadQueueEmptyException
        if dequeue_item.segment.segment_type == 0:
            setattr(dequeue_item, "index", settings.BRAND_SAFETY_VIDEO_INDEX)
            setattr(dequeue_item, "columns", CustomSegmentFileUpload.VIDEO_COLUMNS)
            setattr(dequeue_item, "sort", "views")
        else:
            setattr(dequeue_item, "index", settings.BRAND_SAFETY_CHANNEL_INDEX)
            setattr(dequeue_item, "columns", CustomSegmentFileUpload.CHANNEL_COLUMNS)
            setattr(dequeue_item, "sort", "subscribers")

        # Set max sizes of exports
        if dequeue_item.segment.list_type == 0:
            setattr(dequeue_item, "batch_size", 2000)
            setattr(dequeue_item, "batch_limit", 10)
        else:
            setattr(dequeue_item, "batch_size", 2000)
            setattr(dequeue_item, "batch_limit", 50)
        return dequeue_item


class CustomSegmentFileUploadQueueEmptyException(Exception):
    pass
