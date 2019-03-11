from datetime import datetime
from datetime import timedelta

from django.db import models
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from django.db.models import ForeignKey
from django.contrib.postgres.fields import JSONField

class BaseManager(models.Manager.from_queryset(models.QuerySet)):
    LIFE_TIME_DAYS = 30

    def cleanup(self):
        now = datetime.now().date()
        boundary_date = now - timedelta(self.LIFE_TIME_DAYS)
        queryset = self.filter(date__lt=boundary_date)
        queryset.delete()


class BaseModel(models.Model):
    date = models.DateField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        abstract = True


class VideoAudit(BaseModel):
    video_id = models.CharField(max_length=30, db_index=True)
    video_title = models.CharField(max_length=255)
    channel_id = models.CharField(max_length=30)
    channel_title = models.CharField(max_length=255, default="No title")
    preferred = models.BooleanField()
    impressions = models.BigIntegerField()
    sentiment = models.FloatField(null=True, blank=True, default=None)
    hits = models.BigIntegerField()
    words = models.TextField()
    account_info = models.TextField()

    objects = BaseManager()

    class Meta:
        unique_together = ("date", "video_id")


class KeywordAudit(BaseModel):
    keyword = models.CharField(max_length=255, db_index=True)
    videos = models.BigIntegerField()
    impressions = models.BigIntegerField()

    objects = BaseManager()

    class Meta:
        unique_together = ("date", "keyword")


class AuditIgnoreModel(models.Model):
    id = models.CharField(primary_key=True, max_length=30, db_index=True)

    class Meta:
        abstract = True


class ChannelAuditIgnore(AuditIgnoreModel):
    pass


class VideoAuditIgnore(AuditIgnoreModel):
    pass


class TopicAudit(BaseModel):
    title = models.CharField(max_length=255, unique=True)
    is_running = models.BooleanField(blank=True, default=True, db_index=True)
    from_beginning = models.BooleanField(blank=True, default=False)
    start_cursor = models.BigIntegerField(default=0)
    completed_at = models.DateTimeField(blank=True, null=True, default=None)
    channel_segment = ForeignKey(PersistentSegmentChannel, related_name='related_topic_channel')
    video_segment = ForeignKey(PersistentSegmentVideo, related_name='related_topic_video')

    class Meta:
        index_together = ['is_running', 'from_beginning']


class Keyword(models.Model):
    keyword = models.CharField(max_length=255)
    topic = ForeignKey(TopicAudit, related_name='keywords')

    class Meta:
        unique_together = ['keyword', 'topic']


class APIScriptTracker(models.Model):
    name = models.CharField(max_length=255, unique=True, db_index=True)
    cursor = models.BigIntegerField(default=0)


class YoutubeUser(models.Model):
    channel_id = models.CharField(max_length=255, unique=True, db_index=True)
    name = models.CharField(max_length=255)
    thumbnail_image_url = models.TextField(null=True)


class Comment(models.Model):
    id = models.CharField(primary_key=True, max_length=255)
    user = ForeignKey(YoutubeUser, related_name='comments')
    parent_id = models.CharField(max_length=255, blank=True, null=True)
    video_id = models.CharField(max_length=255)
    published_at = models.DateTimeField()
    updated_at = models.DateTimeField(blank=True, null=True)
    text = models.TextField()
    like_count = models.IntegerField(blank=True, null=True)
    is_top_level = models.BooleanField(default=True)
    reply_count = models.IntegerField(blank=True, null=True)
    time_stamp = models.CharField(max_length=255, default='')
    found_items = JSONField(default={})

    class Meta:
        unique_together = ['id', 'video_id']

