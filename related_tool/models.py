from django.db import models


class RelatedVideo(models.Model):
    video_id = models.CharField(max_length=50, unique=True)
    channel_id = models.CharField(max_length=50)
    channel_title = models.CharField(max_length=225)
    title = models.CharField(max_length=225)
    description = models.TextField()
    scanned = models.BooleanField(default=False, db_index=True)
    source = models.ForeignKey("self", blank=True, null=True, on_delete=models.CASCADE)
