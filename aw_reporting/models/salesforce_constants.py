class SalesForceGoalType:
    CPM = 0
    CPV = 1
    CPM_AND_CPV = 2
    HARD_COST = 3


SalesForceRegions = (
    "West / Northwest",
    "Midwest",
    "East / Southeast",
    "International",
)


class SalesForceRegionId:
    WEST_NORTHWEST = 0
    MIDWEST = 1
    EAST_SOUTHEAST = 2
    INTERNATIONAL = 3


ALL_SALESFORCE_REGIONS = (
    SalesForceRegionId.WEST_NORTHWEST,
    SalesForceRegionId.MIDWEST,
    SalesForceRegionId.EAST_SOUTHEAST,
    SalesForceRegionId.INTERNATIONAL,
)


def salesforce_region_str(region_id):
    try:
        return SalesForceRegions[region_id]
    except (TypeError, IndexError):
        return None


SalesForceGoalTypes = ("CPM", "CPV", "CPM & CPV", "Hard Cost")


def goal_type_str(goal_type_id):
    try:
        return SalesForceGoalTypes[goal_type_id]
    except (TypeError, IndexError):
        return None


class SalesForceGoalTypeStr:
    CPM = goal_type_str(SalesForceGoalType.CPM)
    CPV = goal_type_str(SalesForceGoalType.CPV)
    HARD_COST = goal_type_str(SalesForceGoalType.HARD_COST)


class DynamicPlacementType:
    BUDGET = "Budget"
    SERVICE_FEE = "Service Fee"
    RATE_AND_TECH_FEE = "Rate + Tech Fee"


ALL_DYNAMIC_PLACEMENTS = (
    DynamicPlacementType.BUDGET,
    DynamicPlacementType.SERVICE_FEE,
    DynamicPlacementType.RATE_AND_TECH_FEE
)

DYNAMIC_PLACEMENT_TYPES = (
    DynamicPlacementType.BUDGET,
    DynamicPlacementType.SERVICE_FEE,
    DynamicPlacementType.RATE_AND_TECH_FEE
)
