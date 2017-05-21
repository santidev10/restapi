"""
Segment models module
"""
from django.contrib.postgres.fields import JSONField
from django.db.models import ForeignKey
from django.db.models import ManyToManyField
from django.db.models import Model, CharField

from segment.mini_dash import MiniDashGenerator
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException

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
    title = CharField(max_length=255, null=True, blank=True)
    segment_type = CharField(max_length=255, null=True, blank=True)
    category = CharField(max_length=255, null=True, blank=True)
    statistics = JSONField(default=dict())
    mini_dash_data = JSONField(default=dict())
    owner = ForeignKey('userprofile.userprofile', null=True, blank=True)
    channels = ManyToManyField(
        "segment.ChannelRelation", blank=True, related_name="segments")
    videos = ManyToManyField(
        "segment.VideoRelation", blank=True, related_name="segments")

    def count_statistics_fields(self):
        """
        Setup re-counting of statistics and mini dash data
        """
        if self.segment_type == "channel":
            self.__count_channel_segment_statistics_fields()
        if self.segment_type == "video":
            # TODO add functionality
            return
        if self.segment_type == "keyword":
            # TODO add functionality
            return

    def __count_channel_segment_statistics_fields(self):
        """
        Count statistic for channel segments
        """
        # Drop statistics
        if not self.channels.exists():
            self.statistics = {}
            self.mini_dash_data = {}
            self.save()
            return
        # obtain channels data
        channels_ids = self.channels.values_list(
            "channel_id", flat=True)
        channels_count = self.channels.count()
        # TODO flat may freeze SDB if queryset is too big
        query_params = {"ids": ",".join(channels_ids),
                        "fields": "id,title,thumbnail_image_url,"
                                  "subscribers,video_count,"
                                  "video_views,likes,dislikes,comments,"
                                  "video_views_history,"
                                  "views_per_video_history,description,"
                                  "language,history_date",
                        "flat": 1}
        connector = Connector()
        try:
            response_data = connector.get_channel_list(query_params)
        except SingleDatabaseApiConnectorException:
            # TODO add fail logging and, probably, retries
            return
        # TODO check channels from response and available relations
        # count statistics
        subscribers_count = 0
        videos_count = 0
        views_count = 0
        likes_count = 0
        dislikes_count = 0
        comments_count = 0
        for obj in response_data:
            subscribers_count += obj.get("subscribers")
            videos_count += obj.get("video_count")
            views_count += obj.get("video_views")
            likes_count += obj.get("likes")
            dislikes_count += obj.get("dislikes")
            comments_count += obj.get("comments")
        top_three_channels = sorted(
            response_data, key=lambda k: k['subscribers'], reverse=True)[:3]
        top_three_channels_data = [
            {"id": obj.get("id"),
             "image_url": obj.get("thumbnail_image_url"),
             "title": obj.get("title")}
            for obj in top_three_channels]
        statistics = {
            "top_three_channels": top_three_channels_data,
            "channels_count": channels_count,
            "subscribers_count": subscribers_count,
            "videos_count": videos_count,
            "views_per_channel": views_count / channels_count,
            "subscribers_per_channel": subscribers_count / channels_count,
            "sentiment": (likes_count / max(
                sum((likes_count, dislikes_count)), 1)) * 100,
            "engage_rate": (sum(
                (likes_count, dislikes_count, comments_count))
                            / max(views_count, 1)) * 100
        }
        self.statistics = statistics
        # count mini-dash
        self.mini_dash_data = MiniDashGenerator(response_data).data
        self.save()
