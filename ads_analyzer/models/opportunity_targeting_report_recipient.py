from django.db import models
from django.db.models import CASCADE

from ads_analyzer.models.opportunity_targeting_report import OpportunityTargetingReport
from userprofile.models import UserProfile


class OpportunityTargetingReportRecipient(models.Model):
    report = models.ForeignKey(OpportunityTargetingReport, null=False, on_delete=CASCADE)
    user = models.ForeignKey(UserProfile, null=False, on_delete=CASCADE)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["report", "user"], name="unique_recipient_for_report")
        ]
