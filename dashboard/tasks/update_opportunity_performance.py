from datetime import timedelta
import json

from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.models import Opportunity
from dashboard.models import OpportunityPerformance
from saas import celery_app
from utils.datetime import now_in_default_tz


END_DATE_THRESHOLD = 15
MAX_HISTORY = 30
PERFORMANCE_KEYS = ["margin", "video_view_rate", "viewability", "ctr", "completion_rate", "pacing", "cpv"]


@celery_app.task()
def update_opportunity_performance_task():
    today = now_in_default_tz().date()
    today_str = str(today)
    end_date_threshold = today - timedelta(days=END_DATE_THRESHOLD)
    report = PacingReport()
    filters = {
        "ids": Opportunity.objects.filter(end__gte=end_date_threshold, probability=100).values_list("id", flat=True)
    }
    opportunities = report.get_opportunities(filters)
    for op in opportunities:
        perform_obj, _ = OpportunityPerformance.objects.get_or_create(opportunity_id=op["id"])
        history = perform_obj.history
        # If saving performance for first time or today's performance has not been saved, add to history
        if not history or (history and today_str != history[-1].get("date")):
            today_data = {
                "date": today_str,
                **{key: op.get(key) for key in PERFORMANCE_KEYS},
            }
            # If max length, truncate by 1
            from_index = 0 if len(history) <= MAX_HISTORY else 1
            data = history[from_index:MAX_HISTORY] + [today_data]
            perform_obj.performance = json.dumps(data)
            perform_obj.save()
