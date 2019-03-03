"""
SegmentChannel models module
"""
from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.fields import JSONField
from django.db import models

from aw_reporting.models import YTChannelStatistic
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.settings import DEFAULT_CHANNEL_LIST_SOURCES, \
    DEFAULT_VIDEO_LIST_SOURCES
from .base import BaseSegment
from .base import BaseSegmentRelated
from .base import SegmentManager


class SegmentChannel(BaseSegment):
    PRIVATE = "private"
    YOUTUBE = "youtube"
    IAB = "iab"
    CAS = "cas"
    BLACKLIST = "blacklist"

    CATEGORIES = (
        (PRIVATE, PRIVATE),
        (YOUTUBE, YOUTUBE),
        (IAB, IAB),
        (CAS, CAS),
        (BLACKLIST, BLACKLIST),
    )

    category = models.CharField(max_length=255, choices=CATEGORIES)

    channels = models.BigIntegerField(default=0, db_index=True)
    videos = models.BigIntegerField(default=0, db_index=True)
    top_three_channels = JSONField(default=dict())

    # ---> deprecated
    views_per_channel = models.BigIntegerField(default=0, db_index=True)
    subscribers_per_channel = models.BigIntegerField(default=0, db_index=True)
    subscribers = models.BigIntegerField(default=0, db_index=True)
    views = models.BigIntegerField(default=0, db_index=True)
    likes = models.BigIntegerField(default=0, db_index=True)
    dislikes = models.BigIntegerField(default=0, db_index=True)
    comments = models.BigIntegerField(default=0, db_index=True)
    video_views = models.BigIntegerField(default=0, db_index=True)
    engage_rate = models.FloatField(default=0.0, db_index=True)
    sentiment = models.FloatField(default=0.0, db_index=True)
    if "postgres" in settings.DATABASES["default"]["ENGINE"]:
        top_recommend_channels = ArrayField(
            base_field=models.CharField(max_length=60), default=list, size=None)

    # <--- deprecated

    def load_list(self, query_params):
        return Connector().get_channel_list(query_params=query_params)

    def load_list_batch_generator(self, filters):
        return Connector().get_channel_list_full(filters, fields=["pk"])

    segment_type = 'channel'
    related_aw_statistics_model = YTChannelStatistic

    objects = SegmentManager()

    def obtain_singledb_data(self, ids_hash):
        """
        Execute call to SDB
        """
        params = {
            "ids_hash": ids_hash,
            "fields": "channel_id,title,thumbnail_image_url",
            "sources": DEFAULT_CHANNEL_LIST_SOURCES,
            "sort": "subscribers:desc",
            "size": 3
        }
        top_three_channels_data = self.load_list(query_params=params)

        params = {
            "ids_hash": ids_hash,
            "fields": "videos",
            "sources": DEFAULT_VIDEO_LIST_SOURCES,
            "size": 10000
        }
        base_data = self.load_list(query_params=params)
        data = {
            "top_three_channels_data": top_three_channels_data,
            "base_data": base_data
        }
        return data

    def populate_statistics_fields(self, data):
        """
        Update segment statistics fields
        """
        self.channels = data.get("base_data").get("items_count")
        self.top_three_channels = [
            {"id": obj.get("channel_id"),
             "title": obj.get("title"),
             "image_url": obj.get("thumbnail_image_url")}
            for obj in data.get("top_three_channels_data").get("items")
        ]

        self.videos = sum(
            value.get("videos") or 0
            for value in data.get("base_data").get("items"))

    @property
    def statistics(self):
        """
        Count segment statistics
        """
        statistics = {
            "channels_count": self.channels,
            "top_three_channels": self.top_three_channels,
        }
        return statistics


class SegmentRelatedChannel(BaseSegmentRelated):
    segment = models.ForeignKey(SegmentChannel, related_name='related')
