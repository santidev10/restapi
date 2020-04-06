from django.db import models


class CriterionType(models.Model):
    KEYWORD = "Keyword"
    USER_INTEREST_LIST = "User Interest and List"
    VERTICAL = "Vertical"
    GENDER = "Gender"
    AGE = "Age"
    PLACEMENT = "Placement"
    PARENTAL_STATUS = "Parental status"
    HOUSEHOLD_INCOME = "Household income"
    NONE = "None"
    UNKNOWN = "unknown"

    NAME_CHOICES = (
        (KEYWORD, "Keyword"),
        (USER_INTEREST_LIST, "User Interest and List"),
        (VERTICAL, "Vertical"),
        (GENDER, "Gender"),
        (AGE, "Age"),
        (PLACEMENT, "Placement"),
        (PARENTAL_STATUS, "Parental status"),
        (HOUSEHOLD_INCOME, "Household income"),
        (NONE, "None"),
        (UNKNOWN, "unknown"),
    )
    name = models.CharField(max_length=50, db_index=True, choices=NAME_CHOICES)

    @staticmethod
    def get_mapping_to_id():
        mapping = {
            item.name: item.id
            for item in CriterionType.objects.all()
        }
        return mapping


