import logging
import traceback
import pytz

from datetime import datetime
from datetime import timedelta

from django.conf import settings

from administration.notifications import send_email
from aw_reporting.models import Account
from email_reports.reports import CampaignOverPacing
from email_reports.reports import CampaignUnderMargin
from email_reports.reports import CampaignUnderPacing
from email_reports.reports import DailyCampaignReport
from email_reports.reports import ESMonitoringEmailReport
from email_reports.reports import TechFeeCapExceeded
from email_reports.reports import DailyApexCampaignEmailReport
from saas import celery_app

__all__ = [
    "send_daily_email_reports",
    "notify_opportunity_targeting_report_is_ready",
    "schedule_daily_reports"
]

logger = logging.getLogger(__name__)
HOUR_TO_SEND_DAILY_REPORTS = 6


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
    DailyApexCampaignEmailReport,
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
        send_email(
            subject=subject,
            message=body,
            recipient_list=[email],
        )

@celery_app.task
def schedule_daily_reports(**kwargs):

    timezones = Account.objects.values_list("timezone", flat=True).distinct()
    utc_now = datetime.now(pytz.utc)

    for timezone_name in timezones:
        time_to_execute = get_time_to_execute(utc_now, timezone_name)

        send_daily_email_reports.apply_async(eta=time_to_execute, timezone_name=timezone_name, **kwargs)


def get_time_to_execute(utc_now, timezone_name):
    tz = pytz.timezone(timezone_name)
    tz_dt = tz.localize(datetime(day=utc_now.day, month=utc_now.month, year=utc_now.year,
                               hour=HOUR_TO_SEND_DAILY_REPORTS))
    time_to_execute = tz_dt.astimezone(pytz.utc)
    if time_to_execute < utc_now:
        time_to_execute = time_to_execute + timedelta(days=1)
    return time_to_execute
