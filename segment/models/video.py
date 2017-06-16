"""
SegmentVideo models module
"""
import logging
from django.db.models import CharField
from django.db.models import ForeignKey

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
                'limit': '2000',
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

    category = CharField(max_length=255, choices=CATEGORIES)

    singledb_method = Connector().get_video_list
    singledb_fields = [
        "id",
        "title",
        "description",
        "thumbnail_image_url",
        "views",
        "likes",
        "dislikes",
        "comments",
        "views_history",
        "history_date"
    ]

    segment_type = 'video'

    objects = SegmentVideoManager()

    def calculate_statistics(self, data):
        videos_count = len(data)

        views_count = 0
        likes_count = 0
        dislikes_count = 0
        comments_count = 0
        thirty_days_views_count = 0

        for obj in data:
            views_count += obj.get("views")
            likes_count += obj.get("likes")
            dislikes_count += obj.get("dislikes")
            comments_count += obj.get("comments")
            views_history = obj.get("views_history")
            if views_history:
                thirty_days_views_count += (views_history[:30][0] - views_history[:30][-1])

        top_three_videos = sorted(data, key=lambda k: k['views'], reverse=True)[:3]
        top_three_videos_data = [
            {
                "id": obj.get("id"),
                "image_url": obj.get("thumbnail_image_url"),
                "title": obj.get("title")
            } for obj in top_three_videos
        ]

        views_per_video = views_count / videos_count if videos_count else 0
        statistics = {
            "top_three_videos": top_three_videos_data,
            "videos_count": videos_count,
            "views_count": views_count,
            "views_per_video": views_per_video,
            "thirty_days_views_count": thirty_days_views_count,
            "sentiment": (likes_count / max(sum((likes_count, dislikes_count)), 1)) * 100,
            "engage_rate": (sum((likes_count, dislikes_count, comments_count)) / max(views_count, 1)) * 100,
        }
        return statistics
 

class SegmentRelatedVideo(BaseSegmentRelated):
    segment = ForeignKey(SegmentVideo, related_name='related')
