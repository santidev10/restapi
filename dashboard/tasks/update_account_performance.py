import json

from django.db.models import Sum
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.reports.pacing_report import get_pacing_from_flights
from aw_reporting.models import Campaign
from dashboard.models import AccountPerformance
from aw_reporting.models import Account
from aw_reporting.google_ads.google_ads_updater import GoogleAdsUpdater
from saas import celery_app
from aw_reporting.models import get_video_view_rate
from utils.datetime import now_in_default_tz


END_DATE_THRESHOLD = 15
MAX_HISTORY = 30
PERFORMANCE_KEYS = ["margin", "video_view_rate", "active_view_viewability", "ctr", "video_completion_rates",
                    "pacing", "cpv"]


@celery_app.task()
def update_account_performance_task():
    pacing_report = PacingReport()
    today = now_in_default_tz().date()
    today_str = str(today)
    for account in GoogleAdsUpdater.get_accounts_to_update(as_obj=True, end_date_from_days=END_DATE_THRESHOLD):
        stats = _get_stats(pacing_report, account, today)

        perform_obj, _ = AccountPerformance.objects.get_or_create(account_id=account.id)
        history = perform_obj.history
        # If saving performance for first time or today's performance has not been saved, add to history
        if not history or (history and today_str != history[-1].get("date")):
            today_data = {
                "date": today_str,
                **stats,
            }
            history += [today_data]
            history = history[-MAX_HISTORY:]
            perform_obj.performance = json.dumps(history)
            perform_obj.save()


def _get_stats(pacing_report, account, today):
    flights = pacing_report.get_flights_data(placement__opportunity__aw_cid__contains=account.id)
    plan_cost = sum(f["total_cost"] for f in flights if f["start"] <= today)
    actual_cost = Campaign.objects.filter(account=account).aggregate(Sum("cost"))["cost__sum"]
    delivery_stats = pacing_report.get_delivery_stats_from_flights(flights)

    pacing = get_pacing_from_flights(flights)
    margin = pacing_report.get_margin_from_flights(flights, actual_cost, plan_cost)
    try:
        video_view_rate = get_video_view_rate(account.video_views, account.impressions)
    except ZeroDivisionError:
        video_view_rate = 0
    stats = dict(
        video_view_rate=video_view_rate,
        active_view_viewability=account.active_view_viewability,
        ctr=delivery_stats["ctr"],
        completion_100_rate=account.get_video_completion_rate(100),
        margin=margin,
        pacing=pacing,
        cpv=delivery_stats["cpv"],
    )
    return stats
