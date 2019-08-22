import logging
import traceback

from django.conf import settings

from email_reports.reports import CampaignOverPacing
from email_reports.reports import CampaignUnderMargin
from email_reports.reports import CampaignUnderPacing
from email_reports.reports import DailyCampaignReport
from email_reports.reports import TechFeeCapExceeded
from saas import celery_app

__all__ = [
    "send_daily_email_reports",
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
)
