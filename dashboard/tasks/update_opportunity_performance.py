from datetime import timedelta
import json

from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.models import Opportunity
from dashboard.models import OpportunityPerformance
from saas import celery_app
from utils.datetime import now_in_default_tz


END_DATE_THRESHOLD = 15
MAX_HISTORY = 30
PERFORMANCE_KEYS = ["margin", "video_view_rate", "active_view_viewability", "ctr", "video_completion_rates",
                    "pacing", "cpv"]


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
            history += [today_data]
            history = history[-MAX_HISTORY:]
            perform_obj.performance = json.dumps(history)
            perform_obj.save()


def aggregate_account_statistics():
    from aw_reporting.models import Account
    from django.db.models import Avg
    from django.db.models import F
    aggregated_stats = Account.objects.filter(id__in=[])\
        .annotate(
            completion_100=F("video_views_100_quartile") / F("impressions"),
            view_rate=F("video_views") / F("impressions"),
        )\
        .aggregate(
            video_view_rate=Avg("view_rate"),
            active_view_viewability=Avg("active_view_viewability"),
            ctr_v=Avg("ctr_v"),
            completion_100_rate=Avg("completion_100")
        )
    return aggregated_stats
