from aw_reporting.models import SalesForceGoalType, OpPlacement
from aw_reporting.models.salesforce_constants import DynamicPlacementType


def get_client_cost(goal_type_id, dynamic_placement, placement_type,
                    ordered_rate, impressions, video_views, aw_cost, total_cost,
                    tech_fee):
    if placement_type == OpPlacement.OUTGOING_FEE_TYPE:
        return 0
    if goal_type_id == SalesForceGoalType.HARD_COST:
        return total_cost

    units = video_views if goal_type_id == SalesForceGoalType.CPV \
        else impressions
    if dynamic_placement == DynamicPlacementType.RATE_AND_TECH_FEE:
        aw_rate = aw_cost / units if units else 0
        return units * (aw_rate + tech_fee)
    if dynamic_placement == DynamicPlacementType.BUDGET:
        return aw_cost

    # assume simple CPM/CPV
    norm_rate = ordered_rate / 1000. if goal_type_id == SalesForceGoalType.CPM \
        else ordered_rate
    return norm_rate * units
