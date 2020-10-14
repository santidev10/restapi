from django.db import models
from django.contrib.auth import get_user_model

from utils.db.functions import safe_bulk_create


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
