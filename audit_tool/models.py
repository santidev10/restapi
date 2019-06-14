from datetime import datetime
from datetime import timedelta
from django.utils import timezone
from django.db import IntegrityError
from django.db import models
from segment.models.persistent import PersistentSegmentChannel
from segment.models.persistent import PersistentSegmentVideo
from django.db.models import ForeignKey
from django.db.models import Q
from django.contrib.postgres.fields import JSONField
import hashlib

def get_hash_name(s):
    return int(hashlib.sha256(s.encode('utf-8')).hexdigest(), 16) % 10 ** 8

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
    cursor_id = models.CharField(max_length=50, blank=True, null=True)


class CommentVideo(models.Model):
    video_id = models.CharField(max_length=15, unique=True)


class YoutubeUser(models.Model):
    channel_id = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=30)
    thumbnail_image_url = models.TextField(null=True)

class Comment(models.Model):
    comment_id = models.CharField(max_length=50, unique=True)
    user = ForeignKey(YoutubeUser, related_name='user_comments')
    video = ForeignKey(CommentVideo, related_name='video_comments')
    parent = ForeignKey('self', blank=True, null=True)
    text = models.TextField()
    published_at = models.DateTimeField()
    updated_at = models.DateTimeField(blank=True, null=True)
    like_count = models.IntegerField(default=0, db_index=True)
    reply_count = models.IntegerField(default=0)
    found_items = JSONField(default={})

class AuditProcessor(models.Model):
    AUDIT_TYPES={
      '0': 'Recommendation Engine',
      '1': 'Video Meta Processor',
      '2': 'Channel Meta Processor',
    }

    created = models.DateTimeField(auto_now_add=True, db_index=True)
    started = models.DateTimeField(auto_now_add=False, db_index=True, default=None, null=True)
    updated = models.DateTimeField(auto_now_add=False, default=None, null=True)
    completed = models.DateTimeField(auto_now_add=False, default=None, null=True)
    max_recommended = models.IntegerField(default=100000)
    params = JSONField(default={})
    cached_data = JSONField(default={})
    pause = models.IntegerField(default=0, db_index=True)
    audit_type = models.IntegerField(db_index=True, default=0)

    @staticmethod
    def get(running=None, audit_type=None, num_days=60, output=None):
        all = AuditProcessor.objects.all()
        if audit_type:
            all = all.filter(audit_type=audit_type)
        if running is not None:
            all = all.filter(completed__isnull=running)
        if num_days:
            all = all.filter(Q(completed__isnull=True) | Q(completed__gte=timezone.now() - timedelta(days=num_days)))
        ret = {
            'running': [],
            'completed': []
        }
        for a in all.order_by("pause", "-completed", "id"):
            d = a.to_dict()
            status = 'running'
            if output:
                print(d['id'], d['name'], d['data'], d['percent_done'])
            else:
                if a.completed is not None:
                    status = 'completed'
                ret[status].append(d)
        if not output:
            return ret

    def to_dict(self):
        audit_type = self.params.get('audit_type_original')
        if not audit_type:
            audit_type = self.audit_type
        lang = self.params.get('language')
        if not lang:
            lang = 'en'
        d = {
            'id': self.id,
            'priority': self.pause,
            'completed_time': self.completed,
            'start_time': self.started,
            'created_time': self.created,
            'data': self.cached_data,
            'name': self.params.get('name'),
            'do_videos': self.params.get('do_videos'),
            'audit_type': audit_type,
            'percent_done': 0,
            'language': lang,
            'category': self.params.get('category'),
            'max_recommended': self.max_recommended
        }
        if self.params.get('error'):
            d['error'] = self.params['error']
        if d['data'].get('total') and d['data']['total'] > 0:
            d['percent_done'] = round(100.0 * d['data']['count'] / d['data']['total'], 2)
            if d['percent_done'] > 100:
                d['percent_done'] = 100
        return d

class AuditLanguage(models.Model):
    language = models.CharField(max_length=64, unique=True)

    @staticmethod
    def from_string(in_var):
        db_result, _ = AuditLanguage.objects.get_or_create(language=in_var.lower())
        return db_result

    def __str__(self):
        return self.language

class AuditCategory(models.Model):
    category = models.CharField(max_length=64, unique=True)
    category_display = models.TextField(default=None, null=True)

    @staticmethod
    def get_all():
        res = {}
        for c in AuditCategory.objects.all():
            res[str(c.category)] = c.category_display
        return res

class AuditCountry(models.Model):
    country = models.CharField(max_length=64, unique=True)

class AuditChannel(models.Model):
    channel_id = models.CharField(max_length=50, unique=True)
    channel_id_hash = models.BigIntegerField(default=0, db_index=True)
    processed = models.BooleanField(default=False, db_index=True)

    @staticmethod
    def get_or_create(channel_id):
        channel_id_hash = get_hash_name(channel_id)
        res = AuditChannel.objects.filter(channel_id_hash=channel_id_hash)
        for r in res:
            if r.channel_id == channel_id:
                return r
        try:
            return AuditChannel.objects.create(
                    channel_id=channel_id,
                    channel_id_hash=channel_id_hash
            )
        except IntegrityError as e:
            return AuditChannel.objects.get(channel_id=channel_id)

class AuditChannelMeta(models.Model):
    channel = models.OneToOneField(AuditChannel)
    name = models.CharField(max_length=255, default=None, null=True)
    description = models.TextField(default=None, null=True)
    keywords = models.TextField(default=None, null=True)
    language = models.ForeignKey(AuditLanguage, db_index=True, default=None, null=True, related_name='ac_language')
    default_language = models.ForeignKey(AuditLanguage, db_index=True, default=None, null=True, related_name='ac_default_language')
    country = models.ForeignKey(AuditCountry, db_index=True, default=None, null=True)
    subscribers = models.BigIntegerField(default=0, db_index=True)
    view_count = models.BigIntegerField(default=0, db_index=True)
    video_count = models.BigIntegerField(default=None, db_index=True, null=True)
    emoji = models.BooleanField(default=False, db_index=True)

class AuditVideo(models.Model):
    channel = models.ForeignKey(AuditChannel, db_index=True, default=None, null=True)
    video_id = models.CharField(max_length=50, unique=True)
    video_id_hash = models.BigIntegerField(default=0, db_index=True)

    @staticmethod
    def get_or_create(video_id):
        video_id_hash = get_hash_name(video_id)
        res = AuditVideo.objects.filter(video_id_hash=video_id_hash)
        if len(res) > 0:
            for r in res:
                if r.video_id == video_id:
                    return r
        try:
            return AuditVideo.objects.create(
                    video_id=video_id,
                    video_id_hash=video_id_hash
            )
        except IntegrityError as e:
            return AuditVideo.objects.get(video_id=video_id)

class AuditVideoMeta(models.Model):
    video = models.OneToOneField(AuditVideo)
    name = models.CharField(max_length=255, null=True, default=None)
    description = models.TextField(default=None, null=True)
    keywords = models.TextField(default=None, null=True)
    language = models.ForeignKey(AuditLanguage, db_index=True, default=None, null=True)
    category = models.ForeignKey(AuditCategory, db_index=True, default=None, null=True)
    views = models.BigIntegerField(default=0, db_index=True)
    likes = models.BigIntegerField(default=0, db_index=True)
    dislikes = models.BigIntegerField(default=0, db_index=True)
    emoji = models.BooleanField(default=False, db_index=True)
    publish_date = models.DateTimeField(auto_now_add=False, null=True, default=None, db_index=True)

class AuditVideoProcessor(models.Model):
    audit = models.ForeignKey(AuditProcessor, db_index=True)
    video = models.ForeignKey(AuditVideo, db_index=True, related_name='avp_video')
    video_source = models.ForeignKey(AuditVideo, db_index=True, default=None, null=True, related_name='avp_video_source')
    processed = models.DateTimeField(default=None, null=True, auto_now_add=False, db_index=True)
    clean = models.BooleanField(default=True, db_index=True)
    word_hits = JSONField(default={}, null=True)

    class Meta:
        unique_together = ("audit", "video")

class AuditChannelProcessor(models.Model):
    audit = models.ForeignKey(AuditProcessor, db_index=True)
    channel = models.ForeignKey(AuditChannel, db_index=True, related_name='avp_channel')
    channel_source = models.ForeignKey(AuditChannel, db_index=True, default=None, null=True, related_name='avp_channel_source')
    processed = models.DateTimeField(default=None, null=True, auto_now_add=False, db_index=True)
    clean = models.BooleanField(default=True, db_index=True)
    word_hits = JSONField(default={}, null=True)

    class Meta:
        unique_together = ("audit", "channel")