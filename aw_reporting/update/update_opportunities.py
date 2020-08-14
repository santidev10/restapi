""" Update opportunity statistics """
from aw_reporting.models import Opportunity
from aw_reporting.reports.pacing_report import PacingReport
from saas import celery_app


@celery_app.task
def update_opportunities_task():
    report = PacingReport()
    opportunities = report.get_opportunities({"status": "active"})
    has_alerts = []
    no_alerts = []
    for op in opportunities:
        if len(op.get("alerts", [])) > 0:
            has_alerts.append(op["id"])
        else:
            no_alerts.append(op["id"])
    Opportunity.objects.filter(id__in=has_alerts).update(has_alerts=True)
    Opportunity.objects.filter(id__in=no_alerts).update(has_alerts=False)

