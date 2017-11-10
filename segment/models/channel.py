"""
SegmentChannel models module
"""
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.fields import JSONField
from django.db import models
from django.conf import settings
from aw_reporting.models import YTChannelStatistic
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
    if "postgres" in settings.DATABASES["default"]["ENGINE"]:
        top_recommend_channels = ArrayField(base_field=models.CharField(max_length=60), default=list, size=None)

    singledb_method = Connector().get_channels_statistics

    segment_type = 'channel'
    related_aw_statistics_model = YTChannelStatistic

    objects = SegmentManager()

    def populate_statistics_fields(self, data):
        self.channels = data['count']
        fields = ['subscribers', 'videos', 'views', 'video_views', 'likes', 'dislikes', 'comments']
        for field in fields:
            setattr(self, field, data[field])

        self.views_per_channel = self.views / self.channels if self.channels else 0
        self.subscribers_per_channel = self.subscribers / self.channels if self.channels else 0
        self.sentiment = (self.likes / max(sum((self.likes, self.dislikes)), 1)) * 100
        self.engage_rate = (sum((self.likes, self.dislikes, self.comments)) / max(self.video_views, 1)) * 100
        self.top_three_channels = data['top_list']
        self.mini_dash_data = data.get("minidash", {})

    @property
    def statistics(self):
        """
        Count segment statistics
        """
        statistics = {
            "top_three_channels": self.top_three_channels,
            "top_recommend_channels": self.top_recommend_channels,
            "channels_count": self.channels,
            "videos_count": self.videos,
            # <--- disabled SAAS-1178
            # "subscribers_count": self.subscribers,
            # "views_per_channel": self.views_per_channel,
            # "subscribers_per_channel": self.subscribers_per_channel,
            # "sentiment": self.sentiment,
            # "engage_rate": self.engage_rate,
            # ---> disabled SAAS-1178
        }
        return statistics


class SegmentRelatedChannel(BaseSegmentRelated):
    segment = models.ForeignKey(SegmentChannel, related_name='related')
