from django.db import models

from .creation import AdGroupCreation


class TargetingItem(models.Model):
    criteria = models.CharField(max_length=150)
    ad_group_creation = models.ForeignKey(
        AdGroupCreation, related_name="targeting_items"
    )
    CHANNEL_TYPE = "channel"
    VIDEO_TYPE = "video"
    TOPIC_TYPE = "topic"
    INTEREST_TYPE = "interest"
    KEYWORD_TYPE = "keyword"
    TYPES = (
        (CHANNEL_TYPE, CHANNEL_TYPE),
        (VIDEO_TYPE, VIDEO_TYPE),
        (TOPIC_TYPE, TOPIC_TYPE),
        (INTEREST_TYPE, INTEREST_TYPE),
        (KEYWORD_TYPE, KEYWORD_TYPE),
    )
    type = models.CharField(max_length=20, choices=TYPES)
    is_negative = models.BooleanField(default=False)

    class Meta:
        unique_together = (('ad_group_creation', 'type', 'criteria'),)
        ordering = ['ad_group_creation', 'type', 'is_negative',
                    'criteria']
