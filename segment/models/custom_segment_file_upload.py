from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import CASCADE
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
    segment = OneToOneField(CustomSegment, related_name="export", on_delete=CASCADE)
    query = JSONField()
    updated_at = DateTimeField(null=True, db_index=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.segment.segment_type == 0:
            self.index = settings.BRAND_SAFETY_VIDEO_INDEX
            self.columns = CustomSegmentFileUpload.VIDEO_COLUMNS
            self.sort = "views"
        else:
            self.index = settings.BRAND_SAFETY_CHANNEL_INDEX
            self.columns = CustomSegmentFileUpload.CHANNEL_COLUMNS
            self.sort = "subscribers"

        # Set max sizes of exports
        self.batch_size = 2000
        if self.segment.list_type == 0:
            self.batch_limit = 10
        else:
            self.batch_limit = 50

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
        return dequeue_item


class CustomSegmentFileUploadQueueEmptyException(Exception):
    pass
