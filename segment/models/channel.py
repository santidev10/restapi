"""
SegmentChannel models module
"""
from django.db.models import CharField
from django.db.models import ForeignKey

from singledb.connector import SingleDatabaseApiConnector as Connector

from .base import BaseSegment
from .base import BaseSegmentRelated


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

    category = CharField(max_length=255, choices=CATEGORIES)

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

    def calculate_statistics(self, data):
        channels_count = len(data)

        subscribers_count = 0
        videos_count = 0
        views_count = 0
        likes_count = 0
        dislikes_count = 0
        comments_count = 0
        video_views_count = 0

        for obj in data:
            subscribers_count += obj.get("subscribers")
            videos_count += obj.get("videos")
            views_count += obj.get("views")
            video_views_count += obj.get("video_views")
            likes_count += obj.get("likes")
            dislikes_count += obj.get("dislikes")
            comments_count += obj.get("comments")

        top_three_channels = sorted(data, key=lambda k: k['subscribers'], reverse=True)[:3]
        top_three_channels_data = [
            {
                "id": obj.get("id"),
                "image_url": obj.get("thumbnail_image_url"),
                "title": obj.get("title")
            } for obj in top_three_channels
        ]

        views_per_channel = 0
        subscribers_per_channel = 0
        if channels_count:
            views_per_channel = views_count / channels_count
            subscribers_per_channel = subscribers_count / channels_count

        statistics = {
            "top_three_channels": top_three_channels_data,
            "channels_count": channels_count,
            "subscribers_count": subscribers_count,
            "videos_count": videos_count,
            "views_per_channel": views_per_channel,
            "subscribers_per_channel": subscribers_per_channel,
            "sentiment": (likes_count / max(sum((likes_count, dislikes_count)), 1)) * 100,
            "engage_rate": (sum((likes_count, dislikes_count, comments_count)) / max(video_views_count, 1)) * 100,
        }
        return statistics


class SegmentRelatedChannel(BaseSegmentRelated):
    segment = ForeignKey(SegmentChannel, related_name='related')
