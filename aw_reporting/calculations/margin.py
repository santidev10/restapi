from datetime import datetime
from datetime import time
from datetime import timedelta

import pytz
from django.conf import settings
from django.db.models import Case
from django.db.models import F
from django.db.models import FloatField
from django.db.models import Sum
from django.db.models import Value
from django.db.models import When

from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import get_margin
from aw_reporting.models.salesforce_constants import DynamicPlacementType


def get_margin_from_flights(flights, cost, plan_cost,
                            allocation_ko=1, campaign_id=None):
    dynamic_placements = list(
        set(f["placement__dynamic_placement"] for f in flights)
    )
    if len(dynamic_placements) == 1 \
            and dynamic_placements[0] == DynamicPlacementType.SERVICE_FEE:
        margin = 1
    else:
        sum_client_cost = 0  # total delivery cost

        for f in flights:
            if campaign_id:
                stats = f["campaigns"].get(campaign_id, {})
            else:
                stats = f

            sum_client_cost += get_client_cost(
                goal_type_id=f["placement__goal_type_id"],
                dynamic_placement=f["placement__dynamic_placement"],
                placement_type=f["placement__placement_type"],
                ordered_rate=f["placement__ordered_rate"] or 0,
                impressions=stats.get("impressions") or 0,
                video_views=stats.get("video_views") or 0,
                aw_cost=stats.get("sum_cost") or 0,
                total_cost=(f["total_cost"] or 0) * allocation_ko,
                tech_fee=float(f["placement__tech_fee"] or 0),
                start=f["start"],
                end=f["end"]
            )

        margin = get_margin(plan_cost=plan_cost, cost=cost,
                            client_cost=sum_client_cost)
    return margin


def get_minutes_run_and_total_minutes(flight):
    minutes_run, total_minutes = None, None
    start_date, start_date = flight["start"], flight["end"]
    if start_date and start_date:
        timezone = pytz.timezone(flight["timezone"] or settings.DEFAULT_TIMEZONE)
        start = datetime.combine(flight["start"], time.min).replace(tzinfo=timezone)
        end = datetime.combine(flight["end"] + timedelta(days=1), time.min).replace(tzinfo=timezone)
        last_update = flight.get("last_update")
        if last_update is not None:
            last_update = last_update.astimezone(timezone)
        else:
            last_update = start
        total_seconds = (end - start).total_seconds()
        latest_datetime = end if end < last_update else last_update
        seconds_run = max((latest_datetime - start).total_seconds(), 0)
        total_minutes = total_seconds // 60
        minutes_run = seconds_run // 60
    return minutes_run, total_minutes


in_flight_dates_criteria = dict(
    placement__adwords_campaigns__statistics__date__gte=F("start"),
    placement__adwords_campaigns__statistics__date__lte=F("end"),
)

flight_statistic_ref = "placement__adwords_campaigns__statistics__"
flight_impressions = flight_statistic_ref + "impressions"
flight_video_views = flight_statistic_ref + "video_views"
flight_cost = flight_statistic_ref + "cost"
flight_delivery_annotate = Sum(
    Case(
        When(
            then=Case(
                When(
                    placement__goal_type_id=Value(
                        SalesForceGoalType.CPM),
                    then=F(flight_impressions),
                ),
                When(
                    placement__goal_type_id=Value(
                        SalesForceGoalType.CPV),
                    then=F(flight_video_views),
                ),
                When(
                    placement__dynamic_placement__in=[
                        DynamicPlacementType.BUDGET,
                        DynamicPlacementType.RATE_AND_TECH_FEE],
                    then=F(flight_cost),
                ),
                output_field=FloatField(),
            ),
            **in_flight_dates_criteria
        ),
    ),
)
