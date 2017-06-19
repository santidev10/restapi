"""
SegmentVideo models module
"""
import logging
from django.contrib.postgres.fields import JSONField
from django.db import models

from singledb.connector import SingleDatabaseApiConnector as Connector

from .base import BaseSegment
from .base import BaseSegmentRelated
from .base import SegmentManager


logger = logging.getLogger(__name__)


class SegmentVideoManager(SegmentManager):
    def update_youtube_segments(self):
        query_params = {'filter': 'categories'}
        filters_categories = Connector().get_video_filters_list(query_params=query_params)
        categories = [i['category'] for i in filters_categories]
        for category in categories:
            logger.info('Updating youtube video-segment by category: {}'.format(category))
            query_params = {
                'sort_by': 'views',
                'fields': 'id',
                'category': category,
                'limit': '10000',
                'preferred_channel': '0',
                'is_monetizable': '1',
                'min_views': '100000',
                'min_sentiment': '80',
                'min_engage_rate': '1',
                'has_lang_code': '1',
            }
            result = Connector().get_video_list(query_params=query_params)
            items = result.get('items', [])
            ids = [i['id'] for i in items]
            segment, created = self.get_or_create(title=category, category=self.model.YOUTUBE)
            segment.replace_related_ids(ids)
            segment.update_statistics(segment)
            logger.info('   ... videos: {}'.format(len(ids)))


class SegmentVideo(BaseSegment):
    YOUTUBE = "youtube"
    BLACKLIST = "blacklist"
    PRIVATE = "private"
    IAB = "iab"

    CATEGORIES = (
        (YOUTUBE, YOUTUBE),
        (BLACKLIST, BLACKLIST),
        (PRIVATE, PRIVATE),
        (IAB, IAB),
    )

    category = models.CharField(max_length=255, choices=CATEGORIES)

    videos = models.BigIntegerField(default=0, db_index=True)
    views_per_video = models.BigIntegerField(default=0, db_index=True)
    views = models.BigIntegerField(default=0, db_index=True)
    likes = models.BigIntegerField(default=0, db_index=True)
    dislikes = models.BigIntegerField(default=0, db_index=True)
    comments = models.BigIntegerField(default=0, db_index=True)
    thirty_days_views = models.BigIntegerField(default=0, db_index=True)
    engage_rate = models.FloatField(default=0.0, db_index=True)
    sentiment = models.FloatField(default=0.0, db_index=True)
    top_three_videos = JSONField(default=dict())

    singledb_method = Connector().get_videos_statistics
    segment_type = 'video'

    objects = SegmentVideoManager()

    def populate_statistics_fields(self, data):
        self.videos = data['count']
        fields = ['views', 'likes', 'dislikes', 'comments', 'thirty_days_views']
        for field in fields:
            setattr(self, field, data[field])

        self.views_per_video = self.views / self.videos if self.videos else 0
        self.sentiment = (self.likes / max(sum((self.likes, self.dislikes)), 1)) * 100
        self.engage_rate = (sum((self.likes, self.dislikes, self.comments)) / max(self.views, 1)) * 100
        self.top_three_videos = data['top_list']
        self.mini_dash_data = data['minidash']

    @property
    def statistics(self):
        statistics = {
            "top_three_videos": self.top_three_videos,
            "videos_count": self.videos,
            "views_count": self.views,
            "views_per_video": self.views_per_video,
            "thirty_days_views_count": self.thirty_days_views,
            "sentiment": self.sentiment,
            "engage_rate": self.engage_rate,
        }
        return statistics
 

class SegmentRelatedVideo(BaseSegmentRelated):
    segment = models.ForeignKey(SegmentVideo, related_name='related')
