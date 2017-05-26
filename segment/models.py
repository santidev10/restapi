"""
Segment models module
"""
from celery import task
from django.contrib.postgres.fields import JSONField
from django.db.models import ForeignKey
from django.db.models import ManyToManyField
from django.db.models import Model, CharField

from segment.mini_dash import SegmentMiniDashGenerator
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException
from utils.models import Timestampable

AVAILABLE_SEGMENT_TYPES = (
    "channel",
    "video",
    "keyword"
)

AVAILABLE_CHANNEL_SEGMENT_CATEGORIES = (
    "private",
    "youtube",
    "iab",
    "cas",
    "blacklist"
)

AVAILABLE_VIDEO_AND_KEYWORD_SEGMENT_CATEGORIES = (
    "channel_factory",
    "blacklist",
    "private"
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


class Segment(Timestampable):
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

    @task
    def count_statistics_fields(self):
        """
        Setup re-counting of statistics and mini dash data
        """
        # we are not using getattr to prevent attribute error
        # and also to set security for update methods
        if self.segment_type == "channel":
            self.__count_channel_segment_statistics_fields()
        elif self.segment_type == "video":
            self.__count_video_segment_statistics_fields()
        elif self.segment_type == "keyword":
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
        # TODO flat may freeze SDB if queryset is too big
        query_params = {"ids": ",".join(channels_ids),
                        "fields": "id,title,thumbnail_image_url,"
                                  "subscribers,videos,views,video_views"
                                  "likes,dislikes,comments,"
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
        # Check all channels still alive in SDB
        response_channels_ids = {obj.get("id") for obj in response_data}
        ids_difference = set(channels_ids) - response_channels_ids
        if ids_difference:
            ChannelRelation.object.filter(id__in=ids_difference).delete()
        channels_count = self.channels.count()
        # all channels we dropped from SDB
        if not channels_count:
            self.statistics = {}
            self.mini_dash_data = {}
            self.save()
            return
        subscribers_count = 0
        videos_count = 0
        views_count = 0
        likes_count = 0
        dislikes_count = 0
        comments_count = 0
        video_views_count = 0
        for obj in response_data:
            subscribers_count += obj.get("subscribers")
            videos_count += obj.get("videos")
            views_count += obj.get("views")
            video_views_count += obj.get("video_views")
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
                            / max(video_views_count, 1)) * 100
        }
        self.statistics = statistics
        # count mini-dash
        self.mini_dash_data = SegmentMiniDashGenerator(
            response_data, self).data
        self.save()

    def __count_video_segment_statistics_fields(self):
        """
        Count statistic for video segments
        """
        # Drop statistics
        if not self.videos.exists():
            self.statistics = {}
            self.mini_dash_data = {}
            self.save()
            return
        # obtain videos data
        videos_ids = self.videos.values_list(
            "video_id", flat=True)
        # TODO flat may freeze SDB if queryset is too big
        query_params = {"ids": ",".join(videos_ids),
                        "fields": "id,title,description,thumbnail_image_url,"
                                  "views,likes,dislikes,"
                                  "comments,views_history,history_date",
                        "flat": 1}
        connector = Connector()
        try:
            response_data = connector.get_video_list(query_params)
        except SingleDatabaseApiConnectorException:
            # TODO add fail logging and, probably, retries
            return
        # Check all videos still alive in SDB
        response_videos_ids = {obj.get("id") for obj in response_data}
        ids_difference = set(videos_ids) - response_videos_ids
        if ids_difference:
            VideoRelation.objects.filter(video_id__in=ids_difference).delete()
        videos_count = self.videos.count()
        # all channels we dropped from SDB
        if not videos_count:
            self.statistics = {}
            self.mini_dash_data = {}
            self.save()
            return
        # count statistics
        views_count = 0
        likes_count = 0
        dislikes_count = 0
        comments_count = 0
        thirty_days_views_count = 0
        for obj in response_data:
            views_count += obj.get("views")
            likes_count += obj.get("likes")
            dislikes_count += obj.get("dislikes")
            comments_count += obj.get("comments")
            views_history = obj.get("views_history")
            if views_history:
                thirty_days_views_count += (
                    views_history[:30][0] - views_history[:30][-1])
        top_three_videos = sorted(
            response_data, key=lambda k: k['views'], reverse=True)[:3]
        top_three_videos_data = [
            {"id": obj.get("id"),
             "image_url": obj.get("thumbnail_image_url"),
             "title": obj.get("title")}
            for obj in top_three_videos]
        statistics = {
            "top_three_videos": top_three_videos_data,
            "videos_count": videos_count,
            "views_count": views_count,
            "views_per_video": views_count / videos_count,
            "thirty_days_views_count": thirty_days_views_count,
            "sentiment": (likes_count / max(
                sum((likes_count, dislikes_count)), 1)) * 100,
            "engage_rate": (sum(
                (likes_count, dislikes_count, comments_count))
                            / max(views_count, 1)) * 100
        }
        self.statistics = statistics
        # count mini-dash
        self.mini_dash_data = SegmentMiniDashGenerator(
            response_data, self).data
        self.save()
