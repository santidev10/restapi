"""
Segment models module
"""
from django.contrib.postgres.fields import JSONField
from django.db.models import ManyToManyField
from django.db.models import Model, CharField

AVAILABLE_SEGMENT_TYPES = (
    "channel",
    "video",
    "keyword"
)

AVAILABLE_SEGMENT_CATEGORIES = (
    "private",
    "youtube",
    "iab",
    "cas",
    "blacklist"
)


class ChannelRelation(Model):
    """
    Relation between segment and channel
    """
    channel_id = CharField(max_length=30, primary_key=True)


class VideoRelation(Model):
    """
    Relation between segment and video
    """
    video_id = CharField(max_length=30, primary_key=True)


class Segment(Model):
    """
    Main segment model
    """
    title = CharField(max_length=255)
    segment_type = CharField(max_length=255)
    category = CharField(max_length=255, default="private")
    statistics = JSONField(default=dict())
    mini_dash_data = JSONField(default=dict())
    channels = ManyToManyField(
        "segment.ChannelRelation", blank=True, related_name="segments")
    videos = ManyToManyField(
        "segment.VideoRelation", blank=True, related_name="segments")
