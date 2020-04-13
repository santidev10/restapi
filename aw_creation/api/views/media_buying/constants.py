from aw_reporting.models import CriterionType


TARGETING_MAPPING = {
    "all": None, # None value implicitly all CriterionType values
    "age": CriterionType.AGE,
    "gender": CriterionType.GENDER,
    "interest": CriterionType.USER_INTEREST_LIST,
    "keyword": CriterionType.KEYWORD,
    "placement": CriterionType.PLACEMENT,
    "topic": CriterionType.VERTICAL,
}
