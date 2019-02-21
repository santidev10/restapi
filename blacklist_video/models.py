from django.db import models

class BlacklistVideo(models.Model):
    video_id = models.CharField(max_length=225)
    channel_id = models.CharField(max_length=225)
    channel_title = models.CharField(max_length=225)
    title = models.CharField(max_length=225)
    description = models.TextField()
    scanned = models.BooleanField(default=False)
