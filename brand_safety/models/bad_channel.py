from django.db import models

from utils.db.models import SoftDeleteModel
from utils.utils import get_all_class_constants


class BadChannel(SoftDeleteModel):
    id = models.AutoField(primary_key=True)
    youtube_id = models.CharField(max_length=30)
    title = models.CharField(max_length=80)
    thumbnail_url = models.URLField()
    category = models.CharField(max_length=80)
    reason = models.CharField(max_length=80, null=True)

    class Meta:
        unique_together = ("youtube_id", "category")


class BadChannelCategory:
    CF_MASTER_EXCLUSION_LIST = "CF Master Exclusion List"
    NEGATIVE_KIDS_PLACEMENT_LIST = "Negative Kids Placement List"


ALL_BAD_CHANNEL_CATEGORIES = get_all_class_constants(BadChannelCategory)
