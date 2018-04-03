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
