from urllib.parse import unquote

from django.contrib.postgres.fields import JSONField
from django.db.models import CASCADE
from django.db.models import DateTimeField
from django.db.models import IntegerField
from django.db.models import Model
from django.db.models import OneToOneField
from django.db.models import TextField
from elasticsearch_dsl import Q

from es_components.config import CHANNEL_INDEX_NAME
from es_components.config import VIDEO_INDEX_NAME
from segment.models.constants import SourceListType
from segment.models.custom_segment import CustomSegment


class CustomSegmentFileUpload(Model):
    BASE_COLUMNS = ["URL", "Title", "Language", "Category", "Overall_Score"]
    CHANNEL_COLUMNS = BASE_COLUMNS + ["Subscribers"]
    VIDEO_COLUMNS = BASE_COLUMNS + ["Views"]

    completed_at = DateTimeField(null=True, default=None, db_index=True)
    created_at = DateTimeField(auto_now_add=True, db_index=True)
    download_url = TextField(null=True)
    segment = OneToOneField(CustomSegment, related_name="export", on_delete=CASCADE)
    query = JSONField()
    updated_at = DateTimeField(null=True, db_index=True)
    filename = TextField(null=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.segment.segment_type == 0:
            self.index = VIDEO_INDEX_NAME
            self.columns = CustomSegmentFileUpload.VIDEO_COLUMNS
            self.sort = "stats.views"
        else:
            self.index = CHANNEL_INDEX_NAME
            self.columns = CustomSegmentFileUpload.CHANNEL_COLUMNS
            self.sort = "stats.subscribers"

        # Set max sizes of exports
        self.batch_size = 2000
        if self.segment.list_type == 0:
            self.batch_limit = 10
        else:
            self.batch_limit = 50

    @property
    def query_obj(self):
        """
        Map JSON query to Elasticsearch Q object
        :return:
        """
        return Q(self.query)

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

    def parse_download_url(self):
        try:
            s3_key = unquote(self.download_url.split(".com/")[1].split("?X-Amz-Algorithm")[0])
        except AttributeError:
            s3_key = None
        return s3_key


class CustomSegmentVettedFileUpload(Model):
    completed_at = DateTimeField(null=True, default=None)
    created_at = DateTimeField(auto_now_add=True)
    download_url = TextField(null=True)
    segment = OneToOneField(CustomSegment, related_name="vetted_export", on_delete=CASCADE)
    filename = TextField(null=True)


class CustomSegmentSourceFileUpload(Model):
    SOURCE_TYPE_CHOICES = (
        [SourceListType.INCLUSION, "inclusion"],
        [SourceListType.EXCLUSION, "exclusion"],
    )
    source_type = IntegerField(choices=SOURCE_TYPE_CHOICES)
    segment = OneToOneField(CustomSegment, related_name="source", on_delete=CASCADE)
    filename = TextField(null=True)


class CustomSegmentFileUploadQueueEmptyException(Exception):
    pass
