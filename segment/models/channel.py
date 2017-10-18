"""
SegmentChannel models module
"""
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.fields import JSONField
from django.db import models
from django.db.models import Sum, Case, When, IntegerField

from aw_reporting.models import Account, YTChannelStatistic, \
    dict_add_calculated_stats
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
    top_recommend_channels = ArrayField(base_field=models.CharField(max_length=60), default=list, size=None)

    singledb_method = Connector().get_channels_statistics

    segment_type = 'channel'

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
        self.mini_dash_data = data['minidash']

    def get_statistics(self, **kwargs):
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
        # obtain user from kwargs -> serializer context -> request
        user = kwargs.get("request").user
        # obtain related to segment channels ids
        channels_ids = SegmentRelatedChannel.objects.filter(
            segment_id=self.id).values_list("related_id", flat=True)
        # obtain aw account
        accounts = Account.user_objects(user)
        # prepare aggregated statistics
        aggregated_data = YTChannelStatistic.objects.filter(
            ad_group__campaign__account__in=accounts,
            yt_id__in=channels_ids).aggregate(
            cost=Sum("cost"), video_views=Sum("video_views"),
            clicks=Sum("clicks"), impressions=Sum("impressions"),
            video_impressions=Sum(Case(When(
                        ad_group__video_views__gt=0,
                        then="impressions",
                    ), output_field=IntegerField())))
        # count and add statistics fields
        dict_add_calculated_stats(aggregated_data)
        # clean up
        fields_to_clean_up = [
            "cost",
            "video_views",
            "clicks",
            "impressions",
            "video_impressions",
            "average_cpm"
        ]
        [aggregated_data.pop(key, None) for key in fields_to_clean_up]
        # finalize statistics data
        statistics.update(aggregated_data)
        return statistics


class SegmentRelatedChannel(BaseSegmentRelated):
    segment = models.ForeignKey(SegmentChannel, related_name='related')
