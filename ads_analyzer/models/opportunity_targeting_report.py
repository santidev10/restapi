from django.contrib.auth import get_user_model
from django.db import models
from django.db.models import CASCADE
from django.db.models.signals import post_save
from django.dispatch import receiver

from ads_analyzer.tasks import create_opportunity_targeting_report
from aw_reporting.models import Opportunity


class OpportunityTargetingReport(models.Model):
    opportunity = models.ForeignKey(Opportunity, null=False, on_delete=CASCADE)
    date_from = models.DateField(null=False)
    date_to = models.DateField(null=False)
    external_link = models.URLField(default=None, null=True)
    recipients = models.ManyToManyField(get_user_model())

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["opportunity", "date_from", "date_to"], name="unique_id_date_range")
        ]


@receiver(post_save, sender=OpportunityTargetingReport, dispatch_uid="save_opportunity_report_receiver")
def save_account_receiver(sender, instance, created, **_):
    if created:
        report = instance
        create_opportunity_targeting_report.apply_async(
            opportunity_id=report.opportunity_id,
            date_from=report.date_from.isoformat(),
            date_to=report.date_to.isoformat(),
        )
