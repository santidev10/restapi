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
    date_from = models.DateField(null=False)
    date_to = models.DateField(null=False)
    s3_file_key = models.CharField(max_length=128, default=None, null=True)
    status = models.CharField(max_length=32, default=ReportStatus.IN_PROGRESS.value,
                              null=False, choices=ReportStatus.choices())
    recipients = models.ManyToManyField(get_user_model())

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["opportunity", "date_from", "date_to"], name="unique_id_date_range")
        ]


@receiver(post_save, sender=OpportunityTargetingReport, dispatch_uid="save_opportunity_report_receiver")
def save_account_receiver(sender, instance, created, **_):
    if created:
        from ads_analyzer.tasks import create_opportunity_targeting_report
        report = instance
        create_opportunity_targeting_report.si(
            opportunity_id=report.opportunity_id,
            date_from_str=report.date_from.isoformat(),
            date_to_str=report.date_to.isoformat(),
        ).apply_async()
