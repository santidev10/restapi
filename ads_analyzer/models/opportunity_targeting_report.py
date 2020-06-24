from django.contrib.auth import get_user_model
from django.db import models
from django.db.models import CASCADE

from aw_reporting.models import Opportunity
from utils.lang import ExtendedEnum


class ReportStatus(ExtendedEnum):
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"


class OpportunityTargetingReport(models.Model):
    opportunity = models.ForeignKey(Opportunity, null=False, on_delete=CASCADE)
    date_from = models.DateField(null=True)
    date_to = models.DateField(null=True)
    s3_file_key = models.CharField(max_length=256, default=None, null=True)
    status = models.CharField(max_length=32, default=ReportStatus.IN_PROGRESS.value,
                              null=False, choices=ReportStatus.choices())
    recipients = models.ManyToManyField(get_user_model(), related_name="opportunity_target_reports")
    created_at = models.DateTimeField(auto_now_add=True, null=True)
