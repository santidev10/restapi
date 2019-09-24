from django.db import models
from django.db.models import CASCADE

from aw_reporting.models import Opportunity


class OpportunityTargetingReport(models.Model):
    opportunity = models.ForeignKey(Opportunity, null=False, on_delete=CASCADE)
    date_from = models.DateField(null=False)
    date_to = models.DateField(null=False)
    external_link = models.URLField(default=None, null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["opportunity", "date_from", "date_to"], name="unique_id_date_range")
        ]
