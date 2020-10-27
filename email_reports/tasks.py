import logging
import traceback
from datetime import datetime

import pytz
from django.conf import settings

from administration.notifications import send_email
from aw_reporting.models import Account
from email_reports.reports import CampaignOverPacing
from email_reports.reports import CampaignUnderMargin
from email_reports.reports import CampaignUnderPacing
from email_reports.reports import DailyApexVisaCampaignEmailReport
from email_reports.reports import ESMonitoringEmailReport
from email_reports.reports import FlightDeliveredReport
from email_reports.reports import TechFeeCapExceeded
from saas import celery_app
from utils.datetime import from_local_to_utc

__all__ = [
    "send_daily_email_reports",
    "notify_opportunity_targeting_report_is_ready",
    "schedule_daily_reports"
]

logger = logging.getLogger(__name__)
HOUR_SEND_DAILY_REPORTS = 6


@celery_app.task
def send_daily_email_reports(reports=None, margin_bound=None, days_to_end=None, fake_tech_fee_cap=None, roles=None,
                             debug=settings.DEBUG_EMAIL_NOTIFICATIONS, timezone_name=None):
    kwargs = dict(
        host=settings.HOST,
        debug=debug,
        margin_bound=margin_bound,
        days_to_end=days_to_end,
        fake_tech_fee_cap=fake_tech_fee_cap,
        roles=roles,
        timezone_name=timezone_name,
    )

    if not reports:
        return

    for report_class in EMAIL_REPORT_CLASSES:
        if reports and report_class.__name__ not in reports:
            continue
        try:
            report = report_class(**kwargs)
            report.send()
        # pylint: disable=broad-except
        except Exception as e:
            # pylint: enable=broad-except
            logger.critical("Worker got error: %s", str(e))
            logger.critical(traceback.format_exc())


EMAIL_REPORT_CLASSES = (
    DailyApexVisaCampaignEmailReport,
    CampaignUnderMargin,
    FlightDeliveredReport,
    TechFeeCapExceeded,
    CampaignUnderPacing,
    CampaignOverPacing,
    ESMonitoringEmailReport,
)


@celery_app.task
def notify_opportunity_targeting_report_is_ready(report_id):
    # pylint: disable=import-outside-toplevel
    from ads_analyzer.models import OpportunityTargetingReport
    from ads_analyzer.reports.opportunity_targeting_report.s3_exporter import OpportunityTargetingReportS3Exporter
    # pylint: enable=import-outside-toplevel
    try:
        report = OpportunityTargetingReport.objects.get(pk=report_id)
    except OpportunityTargetingReport.DoesNotExist:
        return
    direct_link = OpportunityTargetingReportS3Exporter.generate_temporary_url(report.s3_file_key)
    subject = f"Opportunity Targeting Report > {report.opportunity.name}: {report.date_from} - {report.date_to}"
    body = f"Report has been prepared. Download it by the following link {direct_link}"
    for email in report.recipients.all().values_list("email", flat=True):
        send_email(
            subject=subject,
            message=body,
            recipient_list=[email],
        )


@celery_app.task
def schedule_daily_reports(**kwargs):
    utc_now = datetime.now(pytz.utc)
    local_execution_time = datetime(day=utc_now.day, month=utc_now.month, year=utc_now.year,
                                    hour=HOUR_SEND_DAILY_REPORTS, )

    timezones = Account.objects.values_list("timezone", flat=True).distinct()

    for timezone_name in timezones:
        time_to_execute = from_local_to_utc(utc_now, timezone_name, local_execution_time)

        send_daily_email_reports.apply_async(eta=time_to_execute, kwargs=dict(timezone_name=timezone_name, **kwargs))
