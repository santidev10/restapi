import json

from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db import models

from aw_reporting.models import Opportunity
from utils.models import Timestampable


class OpportunityWatch(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    opportunity = models.ForeignKey(Opportunity, on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, related_name="watch", on_delete=models.CASCADE)

    class Meta:
        unique_together = (("opportunity", "user"),)


class OpportunityPerformance(Timestampable):
    opportunity = models.OneToOneField(Opportunity, related_name="performance", on_delete=models.CASCADE)
    performance = JSONField(default=list)

    @property
    def history(self):
        data = json.loads(self.performance or "[]")
        return data
