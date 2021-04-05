from django.db import models
from django.contrib.auth import get_user_model
from django.conf import settings

from .custom_segment import CustomSegment
from brand_safety.constants import CHANNEL
from brand_safety.constants import VIDEO
from segment.models.constants import SegmentTypeEnum
from utils.db.functions import safe_bulk_create
from utils.models import Timestampable


class SegmentAction(models.Model):
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    action = models.IntegerField(db_index=True)
    user = models.ForeignKey(get_user_model(), related_name="ctl_actions", on_delete=models.CASCADE)

    @staticmethod
    def add(user, *action_types):
        to_create = [
            SegmentAction(user=user, action=action) for action in action_types
        ]
        safe_bulk_create(SegmentAction, to_create)


class ParamsTemplate(Timestampable):
    """
    Model to store segment parameters template for users
    """
    SEGMENT_TYPE_CHOICES = (
        (SegmentTypeEnum.VIDEO.value, VIDEO),
        (SegmentTypeEnum.CHANNEL.value, CHANNEL)
    )
    segment_type = models.IntegerField(choices=SEGMENT_TYPE_CHOICES, db_index=True)
    title = models.CharField(max_length=100, db_index=True)
    title_hash = models.BigIntegerField(default=0, db_index=True)
    owner = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    params = models.JSONField(default=dict)

    class Meta:
        unique_together = ("title", "owner", "segment_type")


class SegmentAdGroupSync(Timestampable):
    segment = models.OneToOneField(CustomSegment, related_name="sync", on_delete=models.CASCADE)
    adgroup = models.OneToOneField("oauth.AdGroup", related_name="gads_sync", on_delete=models.CASCADE)
    is_synced = models.BooleanField(default=False, db_index=True)

    class Meta:
        unique_together = ("segment", "adgroup")
