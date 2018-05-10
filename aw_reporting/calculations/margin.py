from django.db.models import Sum, When, Case, Value, F, FloatField

from aw_reporting.models import OpPlacement, SalesForceGoalType, Flight, \
    get_margin
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

            delivery = stats.get("delivery") or 0
            placement_ordered_rate = f["placement__ordered_rate"] or 0

            goal_type_id = f["placement__goal_type_id"]
            dynamic_placement = f["placement__dynamic_placement"]

            if f["placement__placement_type"] \
                    == OpPlacement.OUTGOING_FEE_TYPE:
                continue

            elif dynamic_placement \
                    == DynamicPlacementType.RATE_AND_TECH_FEE:
                tech_fee = float(f["placement__tech_fee"] or 0)

                units = 0
                if goal_type_id == SalesForceGoalType.CPV:
                    units = stats.get("video_views") or 0

                elif goal_type_id == SalesForceGoalType.CPM:
                    units = stats["impressions"] / 1000

                if units:
                    rate = (stats.get("sum_cost") or 0) / units
                    sum_client_cost += units * (rate + tech_fee)

            elif dynamic_placement == DynamicPlacementType.BUDGET:
                sum_client_cost += stats.get("sum_cost") or 0
            elif goal_type_id == SalesForceGoalType.CPV:
                sum_client_cost += delivery * placement_ordered_rate

            elif goal_type_id == SalesForceGoalType.CPM:
                sum_client_cost += delivery / 1000. * placement_ordered_rate

            elif goal_type_id == SalesForceGoalType.HARD_COST:
                sum_client_cost += (f["total_cost"] or 0) * allocation_ko

        margin = get_margin(plan_cost=plan_cost, cost=cost,
                            client_cost=sum_client_cost)
    return margin


def margin_for_opportunity(opportunity):
    flights = Flight.objects.filter(placement__opportunity=opportunity) \
        .annotate(delivery=flight_delivery_annotate) \
        .values(
        "id", "name", "start", "end", "total_cost", "ordered_units",
        "cost", "placement_id", "delivery",
        "placement__goal_type_id", "placement__placement_type",
        "placement__opportunity_id",
        "placement__opportunity__cannot_roll_over",
        "placement__opportunity__budget",
        "placement__dynamic_placement", "placement__ordered_rate",
        "placement__tech_fee", "placement__tech_fee_type",
    )
    cost = sum((f["cost"] or 0) for f in flights)
    plan_cost = sum((f["total_cost"] or 0) for f in flights)
    return get_margin_from_flights(flights, cost, plan_cost)


def get_days_run_and_total_days(flight, yesterday):
    days_run, total_days = None, None
    start, end = flight["start"], flight["end"]
    if start and end:
        total_days = (end - start).days + 1
        latest_day = end if end < yesterday else yesterday
        days_run = (latest_day - start).days + 1
        if days_run < 0:
            days_run = 0
    return days_run, total_days


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
