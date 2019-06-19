from django.conf import settings
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.contrib.postgres.fields import JSONField
from django.db.models import CharField
from django.db.models import DateTimeField
from django.db.models import ForeignKey
from django.db.models import Model
from django.db.models import PositiveIntegerField
from django.db.models import SET_NULL
from django.db.models import TextField

from brand_safety.constants import CHANNEL
from brand_safety.constants import BLACKLIST
from brand_safety.constants import WHITELIST


class CustomSegmentFileUpload(Model):
    BASE_COLUMNS = ["url", "title", "language", "youtube_category", "overall_score"]
    CHANNEL_COLUMNS = BASE_COLUMNS + ["subscribers"]
    VIDEO_COLUMNS = BASE_COLUMNS + ["views"]

    LIST_TYPE_CHOICES = (
        (BLACKLIST, BLACKLIST),
        (WHITELIST, WHITELIST)
    )

    completed_at = DateTimeField(null=True, default=None, db_index=True)
    created_at = DateTimeField(auto_now_add=True, db_index=True)
    download_url = TextField(null=True)
    list_type = CharField(max_length=10, choices=LIST_TYPE_CHOICES)
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=SET_NULL)
    query = JSONField()
    updated_at = DateTimeField(null=True)

    content_type = ForeignKey(ContentType, null=True)
    segment_id = PositiveIntegerField(null=True)
    segment = GenericForeignKey("content_type", "segment_id")

    class Meta:
        unique_together = ("content_type", "segment_id")

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
        if dequeue_item.segment.segment_type == CHANNEL:
            setattr(dequeue_item, "index", settings.BRAND_SAFETY_CHANNEL_INDEX)
            setattr(dequeue_item, "columns", CustomSegmentFileUpload.CHANNEL_COLUMNS)
            setattr(dequeue_item, "sort", "subscribers")
        else:
            setattr(dequeue_item, "index", settings.BRAND_SAFETY_VIDEO_INDEX)
            setattr(dequeue_item, "columns", CustomSegmentFileUpload.VIDEO_COLUMNS)
            setattr(dequeue_item, "sort", "views")

        # Set max sizes of exports
        if dequeue_item.list_type == BLACKLIST:
            setattr(dequeue_item, "batch_size", 1000)
            setattr(dequeue_item, "batch_limit", 100)
        else:
            setattr(dequeue_item, "batch_size", 1000)
            setattr(dequeue_item, "batch_limit", 20)
        return dequeue_item


class CustomSegmentFileUploadQueueEmptyException(Exception):
    pass
