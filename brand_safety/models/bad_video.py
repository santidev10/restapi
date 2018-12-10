from django.db import models

from utils.db.models import SoftDeleteModel


class BadVideo(SoftDeleteModel):
    id = models.AutoField(primary_key=True)
    youtube_id = models.CharField(max_length=30)
    title = models.CharField(max_length=80)
    thumbnail_url = models.URLField()
    category = models.CharField(max_length=80)
    reason = models.CharField(max_length=80, null=True)

    class Meta:
        unique_together = ("youtube_id", "category")
