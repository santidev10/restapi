import hashlib
from utils.models import Timestampable
from django.db import models
from django.db import IntegrityError
from audit_tool.models import AuditLanguage


def get_hash_name(s):
    return int(hashlib.sha256(s.encode('utf-8')).hexdigest(), 16) % 10 ** 8


class WatsonTranscript(Timestampable):
    video_id = models.CharField(max_length=50, unique=True)
    video_id_hash = models.BigIntegerField(default=0, db_index=True)
    transcript = models.TextField(blank=True, null=True, default=None)
    language = models.ForeignKey(AuditLanguage, db_index=True, null=True, default=None,
                                 on_delete=models.CASCADE, related_name='transcript_language')
    submitted = models.DateTimeField(blank=True, null=True, default=None)
    retrieved = models.DateTimeField(blank=True, null=True, default=None)
    job_status = models.CharField(max_length=255, blank=True, null=True, default=None)
    job_id = models.CharField(max_length=255, blank=True, null=True, default=None)

    @staticmethod
    def get_or_create(video_id):
        video_id_hash = get_hash_name(video_id)
        res = WatsonTranscript.objects.filter(video_id_hash=video_id_hash)
        if len(res) > 0:
            for r in res:
                if r.video_id == video_id:
                    return r
        try:
            return WatsonTranscript.objects.create(
                video_id=video_id,
                video_id_hash=video_id_hash
            )
        except IntegrityError as e:
            return WatsonTranscript.objects.get(video_id=video_id)
