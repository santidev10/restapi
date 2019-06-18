from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import CharField
from django.db.models import DateTimeField
from django.db.models import ForeignKey
from django.db.models import Model
from django.db.models import SET_NULL

import brand_safety.constants as constants


class CustomSegmentFileUpload(Model):
    BASE_COLUMNS = ["url", "overall_score", "language", "youtube_category"]
    CHANNEL_COLUMNS = BASE_COLUMNS + ["subscribers"]
    VIDEO_COLUMNS = BASE_COLUMNS + ["views"]

    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=SET_NULL)
    content_type = CharField(max_length=15)
    created_at = DateTimeField(auto_now_add=True, db_index=True)
    completed_at = DateTimeField(null=True, default=None)
    filename = CharField(max_length=200, unique=True)
    query = JSONField()

    @staticmethod
    def enqueue(*_, **kwargs):
        enqueue_item = CustomSegmentFileUpload.objects.create(**kwargs)
        return enqueue_item

    @staticmethod
    def dequeue():
        dequeue_item = CustomSegmentFileUpload.objects.order_by("id").filter(completed_at=None).first()
        if not dequeue_item:
            raise CustomSegmentFileUploadQueueEmpty
        if dequeue_item.content_type == constants.CHANNEL:
            setattr(dequeue_item, "index", settings.BRAND_SAFETY_CHANNEL_INDEX)
            setattr(dequeue_item, "columns", CustomSegmentFileUpload.CHANNEL_COLUMNS)
        else:
            setattr(dequeue_item, "index", settings.BRAND_SAFETY_VIDEO_INDEX)
            setattr(dequeue_item, "columns", CustomSegmentFileUpload.VIDEO_COLUMNS)
        return dequeue_item


class CustomSegmentFileUploadQueueEmpty(Exception):
    pass
