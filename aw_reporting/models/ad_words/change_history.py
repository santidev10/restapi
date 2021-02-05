""" Keep track of changes made in ViewIQ that will be pulled into Google Ads """

from django.db import models
from django.conf import settings


class BaseHistory(models.Model):
    changes = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    sync_at = models.DateTimeField(null=True, default=None, db_index=True)
    owner = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL)

    class Meta:
        abstract = True


class CampaignHistory(BaseHistory):
    campaign = models.ForeignKey("Campaign", related_name="budget_history", null=True, on_delete=models.SET_NULL)

