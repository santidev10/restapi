from django.contrib.auth import get_user_model
from django.db import models
from django.db.models import CASCADE
from django.db.models.signals import post_save
from django.dispatch import receiver

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


# pylint: disable=import-outside-toplevel,unused-argument
@receiver(post_save, sender=OpportunityTargetingReport, dispatch_uid="save_opportunity_report_receiver")
def save_opportunity_report_receiver(sender, instance, created, **_):
    if created:
        from ads_analyzer.tasks import create_opportunity_targeting_report
        report = instance
        create_opportunity_targeting_report.si(report_id=report.pk, ) \
            .apply_async()
# pylint: enable=import-outside-toplevel,unused-argument
