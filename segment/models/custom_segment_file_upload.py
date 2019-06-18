from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import DateTimeField
from django.db.models import ForeignKey
from django.db.models import Model
from django.db.models import SET_NULL
from django.db.models import TextField

from django.db import models
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType

from brand_safety.constants import CHANNEL


class CustomSegmentFileUpload(Model):
    BASE_COLUMNS = ["url", "title", "language", "youtube_category", "overall_score"]
    CHANNEL_COLUMNS = BASE_COLUMNS + ["subscribers"]
    VIDEO_COLUMNS = BASE_COLUMNS + ["views"]

    completed_at = DateTimeField(null=True, default=None)
    created_at = DateTimeField(auto_now_add=True, db_index=True)
    download_url = TextField(null=True)
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=SET_NULL)
    query = JSONField()

    content_type = ForeignKey(ContentType, null=True)
    segment_id = models.PositiveIntegerField(null=True)
    segment = GenericForeignKey("content_type", "segment_id")

    class Meta:
        unique_together = ('content_type', 'segment_id')

    @staticmethod
    def enqueue(*_, **kwargs):
        """
        Interface to create new export entry
        :param _:
        :param kwargs:
        :return:
        """
        enqueue_item = CustomSegmentFileUpload.objects.create(**kwargs)
        return enqueue_item

    @staticmethod
    def dequeue():
        """
        Interface to return first export entry to be processed
        :return:
        """
        dequeue_item = CustomSegmentFileUpload.objects.filter(completed_at=None).order_by("created_at").first()
        if not dequeue_item:
            raise CustomSegmentFileUploadQueueEmptyException
        if dequeue_item.segment.segment_type == CHANNEL:
            setattr(dequeue_item, "index", settings.BRAND_SAFETY_CHANNEL_INDEX)
            setattr(dequeue_item, "columns", CustomSegmentFileUpload.CHANNEL_COLUMNS)
        else:
            setattr(dequeue_item, "index", settings.BRAND_SAFETY_VIDEO_INDEX)
            setattr(dequeue_item, "columns", CustomSegmentFileUpload.VIDEO_COLUMNS)
        return dequeue_item


class CustomSegmentFileUploadQueueEmptyException(Exception):
    pass
