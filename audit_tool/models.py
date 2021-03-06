import hashlib
from datetime import datetime
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.db import IntegrityError
from django.db import models
from django.db.models import ForeignKey
from django.db.models import IntegerField
from django.utils import timezone

from audit_tool.constants import CHOICE_UNKNOWN
from transcripts.constants import AuditVideoTranscriptSourceTypeIdEnum
from es_components.iab_categories import YOUTUBE_TO_IAB_CATEGORIES_MAPPING
from utils.models import Timestampable


def get_hash_name(s):
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest(), 16) % 10 ** 8


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


class APIScriptTracker(models.Model):
    name = models.CharField(max_length=255, unique=True, db_index=True)
    cursor = models.BigIntegerField(default=0)
    cursor_id = models.CharField(max_length=50, blank=True, null=True)
    timestamp = models.DateTimeField(default=None, null=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)


class CommentVideo(models.Model):
    video_id = models.CharField(max_length=15, unique=True)


class YoutubeUser(models.Model):
    channel_id = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=30)
    thumbnail_image_url = models.TextField(null=True)


class Comment(models.Model):
    comment_id = models.CharField(max_length=50, unique=True)
    user = ForeignKey(YoutubeUser, related_name="user_comments", on_delete=models.CASCADE)
    video = ForeignKey(CommentVideo, related_name="video_comments", on_delete=models.CASCADE)
    parent = ForeignKey("self", blank=True, null=True, on_delete=models.CASCADE)
    text = models.TextField()
    published_at = models.DateTimeField()
    updated_at = models.DateTimeField(blank=True, null=True)
    like_count = models.IntegerField(default=0, db_index=True)
    reply_count = models.IntegerField(default=0)
    found_items = models.JSONField(default=dict)


class AuditProcessor(models.Model):
    AUDIT_TYPES = {
        "0": "Reco. Engine",
        "1": "Video Meta",
        "2": "Channel Meta",
    }
    SOURCE_TYPES = {
        "0": "Audit Tool",
        "1": "CTL Creator",
        "2": "CTL with Keywords",
    }

    created = models.DateTimeField(auto_now_add=True, db_index=True)
    started = models.DateTimeField(auto_now_add=False, db_index=True, default=None, null=True)
    updated = models.DateTimeField(auto_now_add=False, default=None, null=True)
    completed = models.DateTimeField(auto_now_add=False, default=None, null=True, db_index=True)
    max_recommended = models.IntegerField(default=100000)
    # this name field is LOWERCASED for searching, use params["name"] for proper capitalization
    name = models.CharField(max_length=255, db_index=True, default=None, null=True)
    params = models.JSONField(default=dict)
    cached_data = models.JSONField(default=dict)
    pause = models.IntegerField(default=0, db_index=True)
    temp_stop = models.BooleanField(default=False, db_index=True)
    audit_type = models.IntegerField(db_index=True, default=0)
    source = models.IntegerField(db_index=True, default=0)
    seed_status = models.IntegerField(db_index=True, default=0)
    machine = models.IntegerField(db_index=True, default=None, null=True)
    thread = models.IntegerField(db_index=True, default=None, null=True)

    class Meta:
        index_together = [
            ("source", "completed", "audit_type"),
        ]

    @property
    def owner(self):
        if self.params.get("user_id"):
            return get_user_model().objects.get(id=self.params["user_id"])
        return None

    def remove_exports(self):
        exports = []
        for b, _ in self.params.items():
            if "export_" in b:
                exports.append(b)
        for export_name in exports:
            del self.params[export_name]
        self.save()

    # pylint: disable=too-many-branches
    @staticmethod
    def get(running=None, audit_type=None, num_days=15, output=None, search=None, export=None, source=None, cursor=None,
            limit=None):
        all_audits = AuditProcessor.objects.all().exclude(source=1)
        if source is not None:
            all_audits = all_audits.filter(source=source)
        if audit_type:
            all_audits = all_audits.filter(audit_type=audit_type)
        if running is not None:
            all_audits = all_audits.filter(completed__isnull=running)
        date_gte = None
        if num_days > 0:
            date_gte = timezone.now() - timedelta(days=num_days)
        if search:
            all_audits = all_audits.filter(name__icontains=search.lower())
        ret = {
            "running": [],
            "completed": []
        }
        audits = []
        if export:
            exports = AuditExporter.objects \
                .filter(completed__isnull=True,
                        audit_id__in=all_audits.values_list("id", flat=True)) \
                .order_by("started",
                          "audit__pause",
                          "id")
            for e in exports:
                if e.audit not in audits:
                    audits.append(e.audit)
            ret["items_count"] = len(audits)
        else:
            ret["items_count"] = all_audits.count()
            all_audits = all_audits.order_by("pause", "-completed", "id")
            if limit:
                start = (cursor - 1) * limit
                all_audits = all_audits[start:start + limit]
            for a in all_audits:
                if not search and date_gte and a.completed and a.completed < date_gte:
                    break
                audits.append(a)
        for a in audits:
            d = a.to_dict()
            status = "running"
            if output:
                print(d["id"], d["name"], d["data"], d["percent_done"])
            else:
                if a.completed is not None:
                    status = "completed"
                ret[status].append(d)
        if not output:
            return ret
        return None

    # pylint: enable=too-many-branches

    def to_dict(self, get_details=False):
        audit_type = self.params.get("audit_type_original")
        if not audit_type:
            audit_type = self.audit_type
        lang = self.params.get("language")
        if lang and isinstance(lang, (str,)):
            lang = [lang]
        try:
            owner = str(self.owner)
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            owner = "N/A"
        d = {
            "id": self.id,
            "owner": owner,
            "priority": self.pause,
            "completed_time": self.completed,
            "start_time": self.started,
            "created_time": self.created,
            "data": self.cached_data,
            "name": self.params.get("name"),
            "do_videos": self.params.get("do_videos"),
            "audit_type": audit_type,
            "percent_done": 0,
            "language": lang,
            "exclusion_size": self.get_exclusion_size(),
            "inclusion_size": self.get_inclusion_size(),
            "category": self.params.get("category"),
            "max_recommended": self.max_recommended,
            "min_likes": self.params.get("min_likes"),
            "max_dislikes": self.params.get("max_dislikes"),
            "min_views": self.params.get("min_views"),
            "min_date": self.params.get("min_date"),
            "resumed": self.params.get("resumed"),
            "stopped": self.params.get("stopped"),
            "paused": self.temp_stop,
            "num_videos": self.get_num_videos(),
            "projected_completion": "Done" if self.completed else self.params.get("projected_completion"),
            "avg_rate_per_minute": self.get_completed_rate() if self.completed else self.params.get(
                "avg_rate_per_minute"),
            "source": self.SOURCE_TYPES[str(self.source)],
            "max_recommended_type": self.params.get("max_recommended_type"),
            "inclusion_hit_count": self.params.get("inclusion_hit_count"),
            "exclusion_hit_count": self.params.get("exclusion_hit_count"),
            "include_unknown_likes": self.params.get("include_unknown_likes"),
            "include_unknown_views": self.params.get("include_unknown_views"),
            "force_data_refresh": self.params.get("force_data_refresh"),
            "override_blocklist": self.params.get("override_blocklist"),
            "seed_status": self.seed_status_text,
        }
        d["export_status"] = self.get_export_status()
        d["has_history"] = self.has_history()
        if get_details:
            d["related_audits"] = self.get_related_audits()
            files = self.params.get("files")
            if files:
                d["source_file"] = files.get("source")
                d["exclusion_file"] = files.get("exclusion")
                d["inclusion_file"] = files.get("inclusion")
        if self.params.get("error"):
            d["error"] = self.params["error"]
        if d["data"].get("total") and d["data"]["total"] > 0:
            d["percent_done"] = round(100.0 * d["data"]["count"] / d["data"]["total"], 2)
            if d["percent_done"] > 100:
                d["percent_done"] = 100
        return d

    @property
    def seed_status_text(self):
        if self.seed_status == 0:
            return "pending"
        elif self.seed_status == 1:
            return "seeding"
        elif self.seed_status == 2:
            return None

    def get_num_videos(self):
        num_videos = self.params.get("num_videos")
        if not self.params.get("do_videos"):
            return 1
        return num_videos

    def get_completed_rate(self):
        first_time = self.started
        last_time = self.completed
        last_count = self.cached_data.get("total", 0)
        first_count = 0
        try:
            diff = (last_time - first_time)
            minutes = (diff.total_seconds() / 60)
            avg_rate_per_minute = (last_count - first_count) / minutes
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            avg_rate_per_minute = "N/A"
        return avg_rate_per_minute

    def get_exclusion_size(self):
        exclusion_size = self.params.get("exclusion_size")
        if exclusion_size is None:
            exclusion = self.params.get("exclusion")
            if exclusion:
                exclusion_size = len(exclusion)
            else:
                exclusion_size = 0
            self.params["exclusion_size"] = exclusion_size
            self.save(update_fields=["params"])
        return exclusion_size

    def get_inclusion_size(self):
        inclusion_size = self.params.get("inclusion_size")
        if inclusion_size is None:
            inclusion = self.params.get("inclusion")
            if inclusion:
                inclusion_size = len(inclusion)
            else:
                inclusion_size = 0
            self.params["inclusion_size"] = inclusion_size
            self.save(update_fields=["params"])
        return inclusion_size

    def get_export_status(self):
        res = {}
        e = AuditExporter.objects.filter(audit=self, completed__isnull=True).order_by("started")
        if e.count() > 0:
            if e[0].started:
                res["status"] = "Processing Export"
                res["percent_done"] = e[0].percent_done
                res["started"] = e[0].started
                res["machine"] = e[0].machine
                res["thread"] = e[0].thread
                try:
                    res["owner"] = str(e[0].owner)
                # pylint: disable=broad-except
                except Exception as e:
                # pylint: enable=broad-except
                    res["owner"] = ""
                try:
                    res["elapsed_time"] = str(timezone.now() - e[0].started).replace(",", "").split(".")[0]
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
            else:
                res["status"] = "Export Queued"
        return res

    def has_history(self):
        if not self.params.get("error") \
            and self.started \
            and (not self.completed
                 or self.completed > timezone.now() - timedelta(hours=1)):
            return True
        return False

    def get_related_audits(self):
        d = []
        r = self.params.get("related_audits")
        if r:
            for related in r:
                try:
                    a = AuditProcessor.objects.get(id=related)
                    if not a.name:
                        a.name = a.params["name"].lower()
                        a.save(update_fields=["name"])
                    d.append({
                        "id": related,
                        "name": a.params["name"]
                    })
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    d.append({
                        "id": related,
                        "name": "deleted audit",
                    })
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
    category_display_iab = models.TextField(default=None, null=True)

    @staticmethod
    def get_all(iab=False, unique=False):
        res = {}
        seen = set()
        for c in AuditCategory.objects.all():
            if not iab:
                res[str(c.category)] = c.category_display
            else:
                if not c.category_display_iab:
                    try:
                        c.category_display_iab = YOUTUBE_TO_IAB_CATEGORIES_MAPPING.get(c.category_display.lower())[-1]
                        c.save(update_fields=["category_display_iab"])
                    # pylint: disable=broad-except
                    except Exception:
                    # pylint: enable=broad-except
                        c.category_display_iab = ""
                        c.save(update_fields=["category_display_iab"])
                if unique is False:
                    res[str(c.category)] = c.category_display_iab
                else:
                    if str(c.category_display_iab) not in seen:
                        res[str(c.category)] = c.category_display_iab
                seen.add(c.category_display_iab)
        return res


class AuditCountry(models.Model):
    country = models.CharField(max_length=64, unique=True)

    @staticmethod
    def from_string(in_var):
        db_result, _ = AuditCountry.objects.get_or_create(country=in_var.upper())
        return db_result


class AuditChannel(models.Model):
    channel_id = models.CharField(max_length=50, unique=True)
    channel_id_hash = models.BigIntegerField(default=0, db_index=True)
    processed_time = models.DateTimeField(default=None, null=True, db_index=True)

    @staticmethod
    def get_or_create(channel_id, create=True, add_meta=True):
        channel_id_hash = get_hash_name(channel_id)
        res = AuditChannel.objects.filter(channel_id_hash=channel_id_hash)
        for r in res:
            if r.channel_id == channel_id:
                return r
        if create:
            try:
                c = AuditChannel.objects.create(
                    channel_id=channel_id,
                    channel_id_hash=channel_id_hash
                )
                if add_meta:
                    try:
                        AuditChannelMeta.objects.create(channel=c)
                    except Exception as e:
                        pass
                return c
            except IntegrityError:
                return AuditChannel.objects.get(channel_id=channel_id)
        return None


class AuditChannelMeta(models.Model):
    channel = models.OneToOneField(AuditChannel, on_delete=models.CASCADE)
    name = models.TextField(default=None, null=True)
    description = models.TextField(default=None, null=True)
    keywords = models.TextField(default=None, null=True)
    language = models.ForeignKey(AuditLanguage, db_index=True, default=None, null=True, related_name="ac_language",
                                 on_delete=models.CASCADE)
    default_language = models.ForeignKey(AuditLanguage, db_index=True, default=None, null=True,
                                         related_name="ac_default_language", on_delete=models.CASCADE)
    country = models.ForeignKey(AuditCountry, db_index=True, default=None, null=True, on_delete=models.CASCADE)
    subscribers = models.BigIntegerField(default=0, db_index=True)
    view_count = models.BigIntegerField(default=0, db_index=True)
    video_count = models.BigIntegerField(default=None, db_index=True, null=True)
    emoji = models.BooleanField(default=False, db_index=True)
    monetised = models.BooleanField(default=None, null=True)
    last_uploaded = models.DateTimeField(default=None, null=True, db_index=True)
    last_uploaded_view_count = models.BigIntegerField(default=None, null=True, db_index=True)
    last_uploaded_category = models.ForeignKey(AuditCategory, default=None, null=True, db_index=True,
                                               on_delete=models.CASCADE)
    synced_with_viewiq = models.BooleanField(db_index=True, null=True)
    hidden_subscriber_count = models.BooleanField(default=False)
    primary_video_language = models.ForeignKey(AuditLanguage, db_index=True, default=None, null=True,
                                related_name="ac_video_language", on_delete=models.CASCADE)
    likes = models.IntegerField(default=None, null=True)
    dislikes = models.IntegerField(default=None, null=True)

class AuditVideo(models.Model):
    channel = models.ForeignKey(AuditChannel, db_index=True, default=None, null=True, on_delete=models.CASCADE)
    video_id = models.CharField(max_length=50, unique=True)
    video_id_hash = models.BigIntegerField(default=0, db_index=True)
    processed_time = models.DateTimeField(default=None, null=True, db_index=True)

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
        except IntegrityError:
            return AuditVideo.objects.get(video_id=video_id)


class AuditVideoTranscript(models.Model):
    SOURCE_OPTIONS = {
        AuditVideoTranscriptSourceTypeIdEnum.CUSTOM: "Custom Transcripts",
        AuditVideoTranscriptSourceTypeIdEnum.WATSON: "Watson",
        AuditVideoTranscriptSourceTypeIdEnum.TTS_URL: "Youtube TTS URL",
    }
    video = models.ForeignKey(AuditVideo, on_delete=models.CASCADE)
    language = models.ForeignKey(AuditLanguage, default=None, null=True, on_delete=models.CASCADE)
    transcript = models.TextField(default=None, null=True)
    source = models.IntegerField(default=0, db_index=True)

    class Meta:
        unique_together = ("video", "language")

    @staticmethod
    def get_or_create(video_id, language="en", transcript=None, source=0):
        v = AuditVideo.get_or_create(video_id)
        lang = AuditLanguage.from_string(language) if language else None
        t, _ = AuditVideoTranscript.objects.get_or_create(video=v, language=lang, source=source)
        if transcript:
            t.transcript = transcript
            t.save(update_fields=["transcript"])
        return t

    @staticmethod
    def update_or_create_with_parent(video_id: str, lang_code: str, **update_or_create_kwargs):
        """
        shortcut to update_or_create, but also get_or_creates the requisite fk items
        :param video_id:
        :param lang_code:
        :param update_or_create_kwargs:
        :return:
        """
        audit_video = AuditVideo.get_or_create(video_id)
        audit_language = AuditLanguage.from_string(lang_code)
        audit_video_transcript, _ = AuditVideoTranscript.objects.update_or_create(video=audit_video,
                                                                                  language=audit_language,
                                                                                  **update_or_create_kwargs)
        return audit_video_transcript



class AuditVideoMeta(models.Model):
    video = models.OneToOneField(AuditVideo, on_delete=models.CASCADE)
    name = models.CharField(max_length=255, null=True, default=None)
    description = models.TextField(default=None, null=True)
    keywords = models.TextField(default=None, null=True)
    language = models.ForeignKey(AuditLanguage, db_index=True, default=None, null=True, related_name="av_language",
                                 on_delete=models.CASCADE)
    category = models.ForeignKey(AuditCategory, db_index=True, default=None, null=True, on_delete=models.CASCADE)
    views = models.BigIntegerField(default=0, db_index=True)
    likes = models.BigIntegerField(default=0, db_index=True)
    dislikes = models.BigIntegerField(default=0, db_index=True)
    emoji = models.BooleanField(default=False, db_index=True)
    publish_date = models.DateTimeField(auto_now_add=False, null=True, default=None, db_index=True)
    default_audio_language = models.ForeignKey(AuditLanguage, default=None, null=True, on_delete=models.CASCADE)
    duration = models.CharField(max_length=30, default=None, null=True)
    age_restricted = models.BooleanField(default=None, db_index=True, null=True)
    made_for_kids = models.BooleanField(default=None, db_index=True, null=True)
    aspect_ratio = models.FloatField(null=True, default=None)
    live_broadcast = models.BooleanField(default=None, null=True)

class AuditVideoProcessor(models.Model):
    id = models.BigAutoField(primary_key=True)
    audit = models.ForeignKey(AuditProcessor, db_index=True, on_delete=models.CASCADE)
    video = models.ForeignKey(AuditVideo, db_index=True, related_name="avp_video", on_delete=models.CASCADE)
    video_source = models.ForeignKey(AuditVideo, db_index=True, default=None, null=True,
                                     related_name="avp_video_source", on_delete=models.CASCADE)
    channel = models.ForeignKey(AuditChannel, db_index=True, null=True, default=None, related_name="avp_audit_channel",
                                on_delete=models.CASCADE)
    processed = models.DateTimeField(default=None, null=True, auto_now_add=False, db_index=True)
    clean = models.BooleanField(default=True, db_index=True)
    word_hits = models.JSONField(default=dict, null=True)

    class Meta:
        unique_together = ("audit", "video")
        index_together = [
            ("audit", "processed"),
        ]


class AuditChannelProcessor(models.Model):
    id = models.BigAutoField(primary_key=True)
    audit = models.ForeignKey(AuditProcessor, db_index=True, on_delete=models.CASCADE)
    channel = models.ForeignKey(AuditChannel, db_index=True, related_name="avp_channel", on_delete=models.CASCADE)
    channel_source = models.ForeignKey(AuditChannel, db_index=True, default=None, null=True,
                                       related_name="avp_channel_source", on_delete=models.CASCADE)
    processed = models.DateTimeField(default=None, null=True, auto_now_add=False, db_index=True)
    clean = models.BooleanField(default=True, db_index=True)
    word_hits = models.JSONField(default=dict, null=True)

    class Meta:
        unique_together = ("audit", "channel")
        index_together = [
            ("audit", "processed"),
        ]


class AuditExporter(models.Model):
    audit = models.ForeignKey(AuditProcessor, db_index=True, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True, db_index=True)
    clean = models.BooleanField(default=None, db_index=True, null=True)
    completed = models.DateTimeField(default=None, null=True, db_index=True)
    file_name = models.TextField(default=None, null=True)
    final = models.BooleanField(default=False, db_index=True)
    owner_id = IntegerField(null=True, blank=True)
    export_as_videos = models.BooleanField(default=False, db_index=True)
    export_as_channels = models.BooleanField(default=False, db_index=True)
    export_as_keywords = models.BooleanField(default=False, db_index=True)
    started = models.DateTimeField(auto_now_add=False, null=True, default=None, db_index=True)
    percent_done = models.IntegerField(default=0)
    machine = models.IntegerField(null=True, db_index=True)
    thread = models.IntegerField(null=True, db_index=True)
    last_error = models.TextField(default=None, null=True)
    current_step = models.IntegerField(null=True, default=None)

    class Meta:
        index_together = [
            ("audit", "completed"),
        ]

    STEPS = [
        "getting_categories",
        "delete_blocklist_channels",
        "building_bad_word_category_map",
        "check_legacy",
        "processing_initial_objs",
        "getting_channel_scores",
        "creating_big_dict",
        "preparing_to_move_file",
        "moving_file_to_s3",
        "file_copied",
    ]
    def set_current_step(self, step):
        counter = 0
        for s in self.STEPS:
            if s == step:
                self.current_step = counter
                self.save(update_fields=['current_step'])
                return
            counter += 1

    @staticmethod
    def running():
        for a in AuditExporter.objects.filter(started__isnull=False, completed__isnull=True):
            print(a.to_dict())

    def to_dict(self):
        try:
            owner = str(self.owner)
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            owner = ""
        d = {
            "started": self.started,
            "audit": self.audit_id,
            "audit_name": self.audit.name,
            "machine": self.machine,
            "thread": self.thread,
            "percent_done": self.percent_done,
            "owner": owner,
        }
        return d

    @property
    def owner(self):
        if self.owner_id:
            return get_user_model().objects.get(id=self.owner_id)
        return None

    @owner.setter
    def owner(self, owner):
        if owner:
            self.owner_id = owner.id

class AuditMachine(models.Model):
    machine_number = models.IntegerField(unique=True)
    last_seen = models.DateTimeField(auto_now_add=True, db_index=True)

class AuditProcessorCache(models.Model):
    audit = models.ForeignKey(AuditProcessor, db_index=True, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True, db_index=True)
    count = models.BigIntegerField(default=0, db_index=True)


class BlacklistItem(models.Model):
    VIDEO_ITEM = 0
    CHANNEL_ITEM = 1
    item_type = models.IntegerField(db_index=True)
    item_id = models.CharField(db_index=True, max_length=64)
    item_id_hash = models.BigIntegerField(db_index=True)
    blacklist_category = models.JSONField(default=dict)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)
    processed_by_user_id = IntegerField(null=True, default=None, db_index=True)
    blocked_count = models.IntegerField(default=0, db_index=True)
    unblocked_count = models.IntegerField(default=0, db_index=True)
    blocklist = models.BooleanField(default=True, db_index=True)

    class Meta:
        unique_together = ("item_type", "item_id")

    def to_dict(self):
        d = {
            "categories": self.blacklist_category
        }
        return d

    @staticmethod
    def get_or_create(item_id, item_type):
        b_i = BlacklistItem.get(item_id, item_type)
        if not b_i:
            b_i = BlacklistItem.objects.create(
                item_type=item_type,
                item_id=item_id,
                item_id_hash=get_hash_name(item_id),
            )
            return b_i
        return b_i[0]

    @staticmethod
    def get(item_ids, item_type, to_dict=False, blocklist=True):
        if isinstance(item_ids, (str,)):
            item_ids = [item_ids]
        data = []
        items = BlacklistItem.objects.filter(item_type=item_type,
                                             item_id_hash__in=[get_hash_name(str(_id)) for _id in item_ids],
                                             blocklist=blocklist)
        for item in items:
            if item.item_id in item_ids:
                if to_dict:
                    data.append(item.to_dict())
                else:
                    data.append(item)
        return data


class AuditVet(models.Model):
    audit = models.ForeignKey(AuditProcessor, null=True, db_index=True, on_delete=models.CASCADE)
    clean = models.BooleanField(default=None, db_index=True, null=True)  # determined if suitable by user
    created_at = models.DateTimeField(auto_now_add=True)
    checked_out_at = models.DateTimeField(default=None, null=True, auto_now_add=False, db_index=True)
    processed = models.DateTimeField(default=None, null=True, auto_now_add=False, db_index=True)  # vetted at by user
    processed_by_user_id = IntegerField(null=True, default=None, db_index=True)
    skipped = models.BooleanField(default=None,
                                  db_index=True,
                                  null=True)  # skipped if user unable to view in region, or item was deleted

    class Meta:
        abstract = True


class AuditChannelVet(AuditVet):
    channel = models.ForeignKey(AuditChannel, db_index=True, related_name="channel_vets", null=True, default=None,
                                on_delete=models.CASCADE)

    class Meta:
        unique_together = ("audit", "channel")


class AuditVideoVet(AuditVet):
    video = models.ForeignKey(AuditVideo, db_index=True, related_name="video_vets", null=True, default=None,
                              on_delete=models.CASCADE)

    class Meta:
        unique_together = ("audit", "video")


class AuditContentType(models.Model):
    ID_CHOICES = [
        (0, "UGC"),
        (1, "Broadcast"),
        (2, "Brands"),
    ]
    to_str = dict(ID_CHOICES)
    to_str_with_unknown = dict(ID_CHOICES + [CHOICE_UNKNOWN])
    to_id = {val.lower(): key for key, val in to_str.items()}

    id = models.IntegerField(primary_key=True, choices=ID_CHOICES)
    content_type = models.CharField(max_length=20)

    @staticmethod
    def get(value):
        if isinstance(value, str):
            item_id = AuditContentType.to_id[value.lower()]
        else:
            item_id = value
        item = AuditContentType.objects.get(id=item_id)
        return item


class AuditAgeGroup(models.Model):
    ID_CHOICES = [
        (0, "0 - 3 Toddlers"),
        (1, "4 - 8 Young Kids"),
        (2, "9 - 12 Older Kids"),
        (3, "13 - 17 Teens"),
        (4, "18 - 35 Adults"),
        (5, "36 - 54 Older Adults"),
        (6, "55+ Seniors"),
        # commenting these for now. Removed in task/VIQ2-503
        # (7, "Group - Kids (not teens)"),  # parent=2
        # (8, "Group - Family Friendly"),  # parent=3
    ]
    to_str = dict(ID_CHOICES)
    to_str_with_unknown = dict(ID_CHOICES + [CHOICE_UNKNOWN])
    to_id = {val.lower(): key for key, val in to_str.items()}

    id = models.IntegerField(primary_key=True, choices=ID_CHOICES)
    age_group = models.CharField(max_length=25)
    parent = models.ForeignKey("self", on_delete=models.CASCADE, null=True, default=None, related_name="children")

    @staticmethod
    def get_by_group(top_level_only=False):
        """
        Get age groups with subgroups
        :return: dict
        """
        by_group = [{
            "id": group.id,
            "value": group.age_group,
            "children": [] if top_level_only
            else [{"id": child.id, "value": child.age_group} for child in
                  AuditAgeGroup.objects.filter(parent_id=group.id)]
        } for group in AuditAgeGroup.objects.filter(parent=None)]
        return by_group

    @staticmethod
    def get(value):
        if isinstance(value, (str,)):
            item_id = AuditAgeGroup.to_id[value.lower()]
        else:
            item_id = value
        item = AuditAgeGroup.objects.get(id=item_id)
        return item


class AuditGender(models.Model):
    ID_CHOICES = [
        (0, "Neutral"),
        (1, "Female"),
        (2, "Male"),
    ]
    to_str = dict(ID_CHOICES)
    to_str_with_unknown = dict(ID_CHOICES + [CHOICE_UNKNOWN])
    to_id = {val.lower(): key for key, val in to_str.items()}

    id = models.IntegerField(primary_key=True, choices=ID_CHOICES)
    gender = models.CharField(max_length=15)

    @staticmethod
    def get(value):
        if isinstance(value, (str,)):
            item_id = AuditGender.to_id[value.lower()]
        else:
            item_id = value
        gender = AuditGender.objects.get(id=item_id)
        return gender


class AuditContentQuality(models.Model):
    ID_CHOICES = [
        (0, "Low"),
        (1, "Average"),
        (2, "Premium"),
    ]
    to_str = dict(ID_CHOICES)
    to_str_with_unknown = dict(ID_CHOICES + [CHOICE_UNKNOWN])
    to_id = {val.lower(): key for key, val in to_str.items()}

    id = models.IntegerField(primary_key=True, choices=ID_CHOICES)
    quality = models.CharField(max_length=15)

    @staticmethod
    def get(value):
        if isinstance(value, (str,)):
            item_id = AuditContentQuality.to_id[value.lower()]
        else:
            item_id = value
        quality = AuditContentQuality.objects.get(id=item_id)
        return quality


class IASHistory(Timestampable):
    name = models.CharField(max_length=100)
    started = models.DateTimeField(auto_now_add=True, db_index=True)
    completed = models.DateTimeField(auto_now_add=False, default=None, null=True, db_index=True)

    @staticmethod
    def get_last_ingested_timestamp() -> datetime:
        """
        return the latest/newest completed IASHistory object's `started` timestamp
        used for filtering to get the latest ias verified channels
        :return datetime:
        """
        try:
            return IASHistory.objects.exclude(completed=None).latest("completed").started
        except IASHistory.DoesNotExist:
            return None


class IASChannel(Timestampable):
    channel = models.ForeignKey(AuditChannel, db_index=True, null=True, default=None, on_delete=models.CASCADE)
    ias_verified = models.DateTimeField(db_index=True, auto_now_add=True)

    @staticmethod
    def get_or_create(channel_id, create=True):
        audit_channel = AuditChannel.get_or_create(channel_id)
        res = IASChannel.objects.filter(channel=audit_channel)
        for r in res:
            if r.channel == audit_channel:
                return r
        if create:
            try:
                return IASChannel.objects.create(
                    channel=audit_channel,
                    ias_verified=timezone.now()
                )
            except IntegrityError:
                return IASChannel.objects.get(channel=audit_channel)
        return None
