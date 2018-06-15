from aw_reporting.models.salesforce import OpPlacement
from aw_reporting.models.salesforce_constants import DynamicPlacementType, \
    SalesForceGoalType
from utils.datetime import now_in_default_tz


def get_client_cost(goal_type_id, dynamic_placement, placement_type,
                    ordered_rate, impressions, video_views, aw_cost, total_cost,
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
