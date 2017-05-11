from django.db import models
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.fields import JSONField

from singledb.models.base import Base
from singledb.models.base import Timestampable


class Channel(Timestampable):
    id = models.CharField(max_length=30, primary_key=True)
    title = models.CharField(max_length=255, default="No title")
    description = models.TextField(default="Channel has no description", null=True, blank=True)
    youtube_published_at = models.DateTimeField(null=True, blank=True)
    content_owner = models.CharField(max_length=255, blank=True, null=True)
    youtube_keywords = models.TextField(default="")
    thumbnail_image_url = models.URLField(max_length=255, null=True, blank=True)
    tags = models.TextField(default="")
    emails = models.TextField(default="")
    country = models.CharField(max_length=255, null=True, blank=True)
    category = models.TextField(null=True, blank=True)
    monetization = models.FloatField(default=0.0)
    last_video_update_at = models.DateTimeField(null=True)
    social_links = JSONField(null=True, blank=True)
    social_stats = JSONField(null=True, blank=True)
    is_social_valid = models.BooleanField(default=True)
    is_social_customized = models.BooleanField(default=False)
    social_updated_at = models.DateTimeField(null=True, blank=True)
    from_ad_words = models.BooleanField(default=False)
    lang_codes = models.TextField(default="")
    thirty_days_published_videos = models.BigIntegerField(default=0)
    twelve_months_published_videos = models.BigIntegerField(default=0)
    preferred = models.BooleanField(default=False)
    ptk_value = models.CharField(max_length=255, null=True, blank=True)
    is_content_safe = models.BooleanField(default=True)

    inject_from = 'details'

    class Meta:
        db_table = 'channel_channel'
        managed = False

    def __str__(self):
        return self.title

    @property
    def is_monetizable(self):
        return self.monetization > 0


class ChannelDetails(Base):
    channel = models.OneToOneField('singledb.Channel', related_name='details', primary_key=True)
    subscribers = models.BigIntegerField(default=0, db_index=True)
    views = models.BigIntegerField(default=0)
    video_count = models.BigIntegerField(default=0)
    videos = models.BigIntegerField(default=0)
    video_views = models.BigIntegerField(default=0)
    likes = models.BigIntegerField(default=0)
    dislikes = models.BigIntegerField(default=0)
    comments = models.BigIntegerField(default=0)
    views_per_video = models.BigIntegerField(default=0)
    engage_rate = models.FloatField(default=0.0, db_index=True)
    sentiment = models.FloatField(default=0.0, db_index=True)
    history_date = models.DateField()
    history_started_at = models.DateField(null=True, blank=True)
    thirty_days_views = models.BigIntegerField(default=0)
    thirty_days_videos = models.BigIntegerField(default=0)
    thirty_days_subscribers = models.BigIntegerField(default=0)
    analytics = JSONField(default=dict)
    subscribers_history = ArrayField(models.BigIntegerField(default=0), default=list)
    views_history = ArrayField(models.BigIntegerField(default=0), default=list)
    video_count_history = ArrayField(models.BigIntegerField(default=0), default=list)
    videos_history = ArrayField(models.BigIntegerField(default=0), default=list)
    video_views_history = ArrayField(models.BigIntegerField(default=0), default=list)
    likes_history = ArrayField(models.BigIntegerField(default=0), default=list)
    dislikes_history = ArrayField(models.BigIntegerField(default=0), default=list)
    comments_history = ArrayField(models.BigIntegerField(default=0), default=list)
    views_per_video_history = ArrayField(models.BigIntegerField(default=0), default=list)
    engage_rate_history = ArrayField(models.FloatField(default=0.0), default=list)
    sentiment_history = ArrayField(models.FloatField(default=0.0), default=list)

    class Meta:
        db_table = 'channel_channeldetails'
        managed = False
