from datetime import datetime
from datetime import timedelta
from django.contrib.auth import get_user_model
from django.db import models
from django.db.models import CASCADE

from aw_reporting.models import Opportunity


class OpportunityTargetingReport(models.Model):
    opportunity = models.ForeignKey(Opportunity, null=False, on_delete=CASCADE)
    date_from = models.DateField(null=False)
    date_to = models.DateField(null=False)
    external_link = models.URLField(default=None, null=True)
    recipients = models.ManyToManyField(get_user_model())
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    expire_at = models.DateField(null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["opportunity", "date_from", "date_to", "expire_at"], name="unique_id_date_range"
            )
        ]
