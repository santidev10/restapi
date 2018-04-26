from utils.query import Operator

GEO_LOCATION_TIP = "Use /geo_target_list?search= endpoint" \
                   " to get locations and sent their ids back"
GEO_LOCATION_CONDITION = [
    dict(id=Operator.OR, name="Or"),
    dict(id=Operator.AND, name="And"),
]
