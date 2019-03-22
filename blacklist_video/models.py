from django.db import models

class BlacklistVideo(models.Model):
    video_id = models.CharField(max_length=225, unique=True)
    channel_id = models.CharField(max_length=225)
    channel_title = models.CharField(max_length=225)
    title = models.CharField(max_length=225)
    description = models.TextField()
    scanned = models.BooleanField(default=False)
    source = models.ForeignKey('self', blank=True, null=True)

    # def __eq__(self, other):
    #     return self.video_id == other.video_id
    #
    # def __hash__(self):
    #     return hash(self.video_id)
    #
    # def __str__(self):
    #     return self.video_id