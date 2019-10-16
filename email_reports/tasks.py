import logging
import traceback

from django.conf import settings
from django.core.mail import send_mail

from email_reports.reports import CampaignOverPacing
from email_reports.reports import CampaignUnderMargin
from email_reports.reports import CampaignUnderPacing
from email_reports.reports import DailyCampaignReport
from email_reports.reports import ESMonitoringEmailReport
from email_reports.reports import TechFeeCapExceeded
from saas import celery_app

__all__ = [
    "send_daily_email_reports",
    "notify_opportunity_targeting_report_is_ready",
]

logger = logging.getLogger(__name__)


@celery_app.task
def send_daily_email_reports(reports=None, margin_bound=None, days_to_end=None, fake_tech_fee_cap=None, roles=None,
                             debug=True):
    kwargs = dict(
        host=settings.HOST,
        debug=debug,
        margin_bound=margin_bound,
        days_to_end=days_to_end,
        fake_tech_fee_cap=fake_tech_fee_cap,
        roles=roles,
    )

    for report_class in EMAIL_REPORT_CLASSES:
        if reports and report_class.__name__ not in reports:
            continue
        try:
            report = report_class(**kwargs)
            report.send()
        except Exception as e:
            logger.critical('Worker got error: %s' % str(e))
            logger.critical(traceback.format_exc())


EMAIL_REPORT_CLASSES = (
    DailyCampaignReport,
    CampaignUnderMargin,
    TechFeeCapExceeded,
    CampaignUnderPacing,
    CampaignOverPacing,
    ESMonitoringEmailReport,
)


@celery_app.task
def notify_opportunity_targeting_report_is_ready(report_id):
    from ads_analyzer.models import OpportunityTargetingReport
    from ads_analyzer.reports.opportunity_targeting_report.s3_exporter import OpportunityTargetingReportS3Exporter
    try:
        report = OpportunityTargetingReport.objects.get(pk=report_id)
    except OpportunityTargetingReport.DoesNotExist:
        return
    direct_link = OpportunityTargetingReportS3Exporter.generate_temporary_url(report.s3_file_key)
    subject = f"Opportunity Targeting Report > {report.opportunity.name}: {report.date_from} - {report.date_to}"
    body = f"Report has been prepared. Download it by the following link {direct_link}"
    for email in report.recipients.all().values_list("email", flat=True):
        send_mail(
            subject=subject,
            message=body,
            from_email=settings.SENDER_EMAIL_ADDRESS,
            recipient_list=[email],
        )
