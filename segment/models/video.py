"""
SegmentVideo models module
"""
import logging

from django.contrib.postgres.fields import JSONField
from django.db import models

from aw_reporting.models import YTVideoStatistic
from singledb.connector import SingleDatabaseApiConnector as Connector
from .base import BaseSegment
from .base import BaseSegmentRelated
from .base import SegmentManager

logger = logging.getLogger(__name__)


class SegmentVideoManager(SegmentManager):
    def update_youtube_segments(self, force_creation=False):
        query_params = {
            "size": 0,
            "aggregations": "category",
            "fields": "video_id",
            "sources": (),
        }
        response = Connector().get_video_list(query_params=query_params)
        filters_categories = dict(response["aggregations"]["category:count"])
        categories = [k for k, v in filters_categories.items()]
        for category in categories:
            logger.info("Updating youtube video-segment by category: %s",
                        category)

            try:
                segment = self.get(title=category, category=self.model.YOUTUBE)
            except SegmentVideo.DoesNotExist:
                if force_creation:
                    logger.info("Creating new segment \"%s\"", category)
                    segment = self.create(title=category,
                                          category=self.model.YOUTUBE)
                else:
                    logger.warning(
                        "Skipped category \"%s\" - related segment not found",
                        category)
                    continue

            query_params = {
                "sort": "views:desc",
                "fields": "video_id",
                "sources": (),
                "category__terms": category,
                "size": "10000",
                "channel__preferred__term": "false",
                "is_monetizable__term": "true",
                "views__range": "100000,",
                "sentiment__range": "80,",
                "engage_rate__range": "1,",
                "has_lang_code__term": "true",
            }
            result = Connector().get_video_list(query_params=query_params)
            items = result.get("items", [])
            ids = [i["video_id"] for i in items]

            segment.replace_related_ids(ids)
            segment.update_statistics(segment)
            logger.info("   ... videos: %d", len(ids))


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
    top_three_videos = JSONField(default=dict())

    # <--- deprecated
    views_per_video = models.BigIntegerField(default=0, db_index=True)
    views = models.BigIntegerField(default=0, db_index=True)
    likes = models.BigIntegerField(default=0, db_index=True)
    dislikes = models.BigIntegerField(default=0, db_index=True)
    comments = models.BigIntegerField(default=0, db_index=True)
    thirty_days_views = models.BigIntegerField(default=0, db_index=True)
    engage_rate = models.FloatField(default=0.0, db_index=True)
    sentiment = models.FloatField(default=0.0, db_index=True)

    # ---> deprecated

    @property
    def singledb_method(self):
        return Connector().get_video_list

    segment_type = "video"

    objects = SegmentVideoManager()
    related_aw_statistics_model = YTVideoStatistic

    def obtain_singledb_data(self, ids_hash):
        """
        Execute call to SDB
        """
        params = {
            "ids_hash": ids_hash,
            "fields": "video_id,title,thumbnail_image_url",
            "sources": (),
            "sort": "views:desc",
            "size": 3
        }
        return self.singledb_method(query_params=params)

    def populate_statistics_fields(self, data):
        """
        Update segment statistics fields
        """
        self.videos = data.get("items_count")
        self.top_three_videos = [
            {"id": obj.get("video_id"),
             "title": obj.get("title"),
             "image_url": obj.get("thumbnail_image_url")}
            for obj in data.get("items")
        ]

    @property
    def statistics(self):
        """
        Count segment statistics
        """
        statistics = {
            "videos_count": self.videos,
            "top_three_videos": self.top_three_videos,
        }
        return statistics


class SegmentRelatedVideo(BaseSegmentRelated):
    segment = models.ForeignKey(SegmentVideo, related_name="related")
