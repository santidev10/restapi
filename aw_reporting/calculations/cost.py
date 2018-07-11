from django.db.models import Sum, Case, When, F, FloatField

from aw_reporting.models.salesforce import OpPlacement
from aw_reporting.models.salesforce_constants import DynamicPlacementType, \
    SalesForceGoalType
from utils.datetime import now_in_default_tz


def get_client_cost(goal_type_id, dynamic_placement, placement_type,
                    ordered_rate, impressions, video_views, aw_cost,
                    total_cost,
                    tech_fee, start, end):
    if placement_type == OpPlacement.OUTGOING_FEE_TYPE:
        return 0
    if goal_type_id == SalesForceGoalType.HARD_COST:
        total_cost_or_zero = total_cost or 0
        if start is None or end is None:
            return total_cost_or_zero
        today = now_in_default_tz().date()
        total_days = (end - start).days + 1
        days_pass = (min(today, end) - start).days + 1
        return total_cost_or_zero / total_days * days_pass
    video_views = video_views or 0
    impressions = impressions or 0
    units = video_views if goal_type_id == SalesForceGoalType.CPV \
        else impressions
    if dynamic_placement == DynamicPlacementType.RATE_AND_TECH_FEE:
        aw_rate = aw_cost / units if units else 0
        return units * (aw_rate + float(tech_fee))
    if dynamic_placement == DynamicPlacementType.BUDGET:
        return aw_cost or 0

    # assume simple CPM/CPV
    ordered_rate = ordered_rate or 0
    norm_rate = ordered_rate / 1000. if goal_type_id == SalesForceGoalType.CPM \
        else ordered_rate
    return norm_rate * units


def get_client_cost_aggregation(campaign_ref="ad_group__campaign"):
    today = now_in_default_tz().date()

    placement_ref = campaign_ref + "__salesforce_placement"
    start_ref = campaign_ref + "__start_date"
    start_lte_ref = start_ref + "__lte"

    placement_type_ref = placement_ref + "__placement_type"
    dynamic_placement_ref = placement_ref + "__dynamic_placement"
    goal_type_ref = placement_ref + "__goal_type_id"
    total_cost_ref = placement_ref + "__total_cost"
    ordered_rate_ref = placement_ref + "__ordered_rate"
    ordered_rate_isnull = ordered_rate_ref + "__isnull"
    tech_fee_ref = placement_ref + "__tech_fee"

    impressions_ref = "impressions"
    video_views_ref = "video_views"
    aw_cost_ref = "cost"

    then = "then"

    aggregation = Sum(Case(
        # outgoing fee
        When(**{
            placement_type_ref: OpPlacement.OUTGOING_FEE_TYPE,
            then: 0
        }),
        # hard cost
        When(**{
            goal_type_ref: SalesForceGoalType.HARD_COST,
            start_lte_ref: today,
            then: F(total_cost_ref)
        }),
        When(**{
            goal_type_ref: SalesForceGoalType.HARD_COST,
            then: 0
        }),
        # rate and tech fee
        When(**{
            dynamic_placement_ref: DynamicPlacementType.RATE_AND_TECH_FEE,
            goal_type_ref: SalesForceGoalType.CPV,
            then: F(aw_cost_ref) + F(video_views_ref) * F(tech_fee_ref)
        }),
        When(**{
            dynamic_placement_ref: DynamicPlacementType.RATE_AND_TECH_FEE,
            goal_type_ref: SalesForceGoalType.CPM,
            then: F(aw_cost_ref) + F(impressions_ref) * F(tech_fee_ref)
        }),
        # budget
        When(**{
            dynamic_placement_ref: DynamicPlacementType.BUDGET,
            then: F(aw_cost_ref)
        }),
        # regular CPM/CPV
        When(**{
            goal_type_ref: SalesForceGoalType.CPM,
            ordered_rate_isnull: False,
            then: F(impressions_ref) / 1000. * F(ordered_rate_ref),
        }),
        When(**{
            goal_type_ref: SalesForceGoalType.CPV,
            ordered_rate_isnull: False,
            then: F(video_views_ref) * F(ordered_rate_ref),
        }),
        output_field=FloatField()
    ))

    return aggregation
