from aw_reporting.models.salesforce import OpPlacement
from aw_reporting.models.salesforce_constants import DynamicPlacementType, \
    SalesForceGoalType


def get_client_cost(goal_type_id, dynamic_placement, placement_type,
                    ordered_rate, impressions, video_views, aw_cost, total_cost,
                    tech_fee):
    if placement_type == OpPlacement.OUTGOING_FEE_TYPE:
        return 0
    if goal_type_id == SalesForceGoalType.HARD_COST:
        return total_cost or 0
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

