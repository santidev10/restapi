from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db.models import CASCADE
from django.db.models import DateTimeField
from django.db.models import OneToOneField
from django.db.models import Model
from django.db.models import TextField
from elasticsearch_dsl import Q

from es_components.config import CHANNEL_INDEX_NAME
from es_components.config import VIDEO_INDEX_NAME
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.segment.segment_type == 0:
            self.index = VIDEO_INDEX_NAME
            self.columns = CustomSegmentFileUpload.VIDEO_COLUMNS
            self.mapper = self._map_video
            self.sort = "stats.views"
        else:
            self.index = CHANNEL_INDEX_NAME
            self.columns = CustomSegmentFileUpload.CHANNEL_COLUMNS
            self.mapper = self._map_channel
            self.sort = "stats.subscribers"

        # Set max sizes of exports
        self.batch_size = 2000
        if self.segment.list_type == 0:
            self.batch_limit = 10
        else:
            self.batch_limit = 50

    @property
    def query_obj(self):
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

    def _map_video(self, item):
        mapped = {
            "title": item["general_data"].get("title", ""),
            "language": item["general_data"].get("language", ""),
            "category": item["general_data"].get("category", ""),
            "overall_score": item["brand_safety"].get("overall_score", ""),
            "views": item["stats"].get("views", ""),
            "url": "https://www.youtube.com/video/" + item["main"]["id"]
        }
        return mapped

    def _map_channel(self, item):
        mapped = {
            "title": item["general_data"].get("title", ""),
            "language": item["general_data"].get("top_language", ""),
            "category": item["general_data"].get("top_category", ""),
            "overall_score": item["brand_safety"].get("overall_score", ""),
            "subscribers": item["stats"].get("subscribers", ""),
            "url": "https://www.youtube.com/channel/" + item["main"]["id"]
        }
        return mapped



class CustomSegmentFileUploadQueueEmptyException(Exception):
    pass
