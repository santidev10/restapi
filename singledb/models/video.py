from django.db import models
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.fields import JSONField
from django.core.validators import MaxValueValidator
from django.core.validators import MinValueValidator

from singledb.models.base import Base
from singledb.models.base import Timestampable
from utils.constants import LANGUAGES


VIDEO_STATUS_CHOICES = (
    ("Status not set", "Status not set"),
    ("Considering", "Considering"),
    ("Suspended", "Suspended"),
    ("Rejected", "Rejected"),
    ("Unsafe", "Unsafe"),
)


class Video(Timestampable):
    id = models.CharField(max_length=30, primary_key=True)
    channel = models.ForeignKey('singledb.Channel', null=False)
    title = models.CharField(max_length=255, default="No title")
    description = models.TextField(default="Video has no description", null=True, blank=True)
    youtube_published_at = models.DateTimeField(null=True, blank=True)
    thumbnail_image_url = models.URLField(max_length=255, null=True, blank=True)
    tags = models.TextField(default="")
    consideration = models.CharField(max_length=14, choices=VIDEO_STATUS_CHOICES, default="Status not set")
    is_monetizable = models.BooleanField(default=False)
    is_safe = models.BooleanField(default=True)
    is_content_safe = models.BooleanField(default=True)
    country = models.CharField(max_length=255, null=True, blank=True)
    lang_code = models.CharField(max_length=2, null=True, blank=True)
    category = models.CharField(max_length=255, null=True, blank=True)
    transcript = models.TextField(null=True, blank=True)
    top_comments = JSONField(default="[]", null=True, blank=True)
    duration = models.BigIntegerField(null=True, blank=True)
    from_ad_words = models.BooleanField(default=False)
    views = models.BigIntegerField(default=0)
    likes = models.BigIntegerField(default=0)
    dislikes = models.BigIntegerField(default=0)
    comments = models.BigIntegerField(default=0)
    engage_rate = models.FloatField(default=0, db_index=True)
    sentiment = models.FloatField(default=0, db_index=True)
    daily_views = models.BigIntegerField(default=0)
    content_owner = JSONField(null=True)
    ptk_value = models.CharField(max_length=255, null=True, blank=True)
    is_ptk_customized = models.BooleanField(default=False)

    inject_from = 'details'

    class Meta:
        db_table = 'video_video'
        managed = False

    def __str__(self):
        return self.title

    @property
    def language(self):
        if self.lang_code:
            return LANGUAGES.get(self.lang_code)


class VideoDetails(Base):
    video = models.OneToOneField('singledb.Video', related_name="details", primary_key=True)
    history_started_at = models.DateField(null=True, blank=True)
    views_history = ArrayField(models.BigIntegerField(default=0), default=list)
    likes_history = ArrayField(models.BigIntegerField(default=0), default=list)
    dislikes_history = ArrayField(models.BigIntegerField(default=0), default=list)
    comments_history = ArrayField(models.BigIntegerField(default=0), default=list)
    depreciation = models.FloatField(null=True, validators=[MinValueValidator(-.10), MaxValueValidator(-0.01)])
    months_out = models.IntegerField(null=True, validators=[MinValueValidator(3), MaxValueValidator(12)])
    margin = models.FloatField(null=True, validators=[MinValueValidator(.2), MaxValueValidator(.8)])
    cpm = models.FloatField(null=True, validators=[MinValueValidator(.7), MaxValueValidator(1.5)])
    history_date = models.DateField()

    class Meta:
        db_table = 'video_videodetails'
        managed = False


class VideoTrendingBase(models.Model):
    video = models.CharField(max_length=30, db_index=True)
    segment_id = models.IntegerField(null=True)
    rate = models.FloatField(default=0)

    trending_name = None

    class Meta:
        abstract = True
        managed = False
        ordering = ['rate']


class VideoTrendingViral(VideoTrendingBase):
    trending_name = 'viral'

    class Meta:
        db_table = 'trending_viral'


class VideoTrendingTrending(VideoTrendingBase):
    trending_name = 'trending'

    class Meta:
        db_table = 'trending_trending'


class VideoTrendingMostWatched(VideoTrendingBase):
    trending_name = 'most_watched'

    class Meta:
        db_table = 'trending_mostwatched'


class VideoTrendingMostLiked(VideoTrendingBase):
    trending_name = 'most_liked'

    class Meta:
        db_table = 'trending_mostliked'


@property
def TRENDING_MODELS():
    return [m for m in VideoTrendingBase.__subclasses__()]

@property
def TRENDINGS():
    return [m.trending_name for m in TRENDING_MODELS.fget()]

def get_trending_model_by_name(name):
    for model in TRENDING_MODELS.fget():
        if model.trending_name == name:
            return model
    raise ModelDoesNotExist("Invalid name: %s" % name)
