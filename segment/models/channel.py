"""
SegmentChannel models module
"""
from django.contrib.postgres.fields import JSONField
from django.db import models

from singledb.connector import SingleDatabaseApiConnector as Connector

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
    views_per_channel = models.BigIntegerField(default=0, db_index=True)
    subscribers_per_channel = models.BigIntegerField(default=0, db_index=True)
    subscribers = models.BigIntegerField(default=0, db_index=True)
    videos = models.BigIntegerField(default=0, db_index=True)
    views = models.BigIntegerField(default=0, db_index=True)
    likes = models.BigIntegerField(default=0, db_index=True)
    dislikes = models.BigIntegerField(default=0, db_index=True)
    comments = models.BigIntegerField(default=0, db_index=True)
    video_views = models.BigIntegerField(default=0, db_index=True)
    engage_rate = models.FloatField(default=0.0, db_index=True)
    sentiment = models.FloatField(default=0.0, db_index=True)
    top_three_channels = JSONField(default=dict())

    singledb_method =  Connector().get_channel_list
    singledb_fields = [
        "id",
        "title",
        "thumbnail_image_url",
        "subscribers",
        "videos",
        "views",
        "video_views",
        "likes",
        "dislikes",
        "comments",
        "video_views_history",
        "views_per_video_history",
        "description",
        "language",
        "history_date"
    ]

    segment_type = 'channel'

    objects = SegmentManager()

    def populate_statistics_fields(self, data):
        self.channels = len(data)
        self.subscribers = 0
        self.videos = 0
        self.views = 0
        self.video_views = 0
        self.likes = 0
        self.dislikes = 0
        self.comments = 0
        for obj in data:
            self.subscribers += obj.get("subscribers")
            self.videos += obj.get("videos")
            self.views += obj.get("views")
            self.video_views += obj.get("video_views")
            self.likes += obj.get("likes")
            self.dislikes += obj.get("dislikes")
            self.comments += obj.get("comments")

        self.views_per_channel = self.views / self.channels if self.channels else 0
        self.subscribers_per_channel = self.subscribers / self.channels if self.channels else 0
        self.sentiment = (self.likes / max(sum((self.likes, self.dislikes)), 1)) * 100
        self.engage_rate = (sum((self.likes, self.dislikes, self.comments)) / max(self.video_views, 1)) * 100

        self.top_three_channels = [{
            "id": obj.get("id"),
            "image_url": obj.get("thumbnail_image_url"),
            "title": obj.get("title")
        } for obj in sorted(data, key=lambda k: k['subscribers'], reverse=True)[:3]]

    @property
    def statistics(self):
        statistics = {
            "top_three_channels": self.top_three_channels_data,
            "channels_count": self.channels,
            "subscribers_count": self.subscribers,
            "videos_count": self.videos,
            "views_per_channel": self.views_per_channel,
            "subscribers_per_channel": self.subscribers_per_channel,
            "sentiment": self.sentiment,
            "engage_rate": self.engage_rate,
        }
        return statistics


class SegmentRelatedChannel(BaseSegmentRelated):
    segment = models.ForeignKey(SegmentChannel, related_name='related')
