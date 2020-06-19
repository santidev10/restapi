import enum

from django.db import models


class CriteriaTypeEnum(enum.IntEnum):
    # Negative values are not valid enums from Criteria Performance CriteriaType
    VIDEO_CREATIVE = -2
    DEVICE = -1
    KEYWORD = 0
    PLACEMENT = 1
    VERTICAL = 2
    USER_LIST = 3
    USER_INTEREST = 4
    AGE_RANGE = 5
    GENDER = 6
    PARENT = 7
    YOUTUBE_CHANNEL = 8
    YOUTUBE_VIDEO = 9


class CriteriaType(models.Model):
    ID_CHOICES = (
        (CriteriaTypeEnum.KEYWORD.value, "Keyword"),
        (CriteriaTypeEnum.PLACEMENT.value, "Placement"),
        (CriteriaTypeEnum.VERTICAL.value, "Vertical"),
        (CriteriaTypeEnum.USER_LIST.value, "User List"),
        (CriteriaTypeEnum.USER_INTEREST.value, "User Interest"),
        (CriteriaTypeEnum.AGE_RANGE.value, "Age Range"),
        (CriteriaTypeEnum.GENDER.value, "Gender"),
        (CriteriaTypeEnum.PARENT.value, "Parental status"),
        (CriteriaTypeEnum.YOUTUBE_CHANNEL.value, "Youtube Channel"),
        (CriteriaTypeEnum.YOUTUBE_VIDEO.value, "Youtube Video"),
    )
    id = models.IntegerField(primary_key=True, choices=ID_CHOICES)
    name = models.CharField(max_length=20)

    def __str__(self):
        value = self.name
        return value


class AdGroupTargeting(models.Model):
    ad_group = models.ForeignKey("AdGroup", related_name="targeting", on_delete=models.CASCADE)
    type = models.ForeignKey("CriteriaType", on_delete=models.CASCADE)
    is_negative = models.BooleanField(default=False)
    criteria = models.CharField(max_length=150)
    sync_pending = models.BooleanField(default=False, db_index=True)
    # statistics_criteria are values stored in statistical models (AgeRangeStatistic.age_range_id,
    # GenderStatistic.gender_id, etc) that will be used to match AdGroupTargeting objects
    statistic_criteria = models.CharField(max_length=150, db_index=True)

    PAUSED = 0
    ENABLED = 1
    REMOVED = 2
    STATUS_CHOICES = (
        (PAUSED, "paused"),
        (ENABLED, "enabled"),
        (REMOVED, "removed"),
    )
    status = models.IntegerField(choices=STATUS_CHOICES)

    class Meta:
        unique_together = (('ad_group', 'type', 'criteria'),)


class TargetingStatusEnum(enum.IntEnum):
    # Negative values are not valid enums from Criteria Performance Status
    EXCLUDED = - 1
    PAUSED = 0
    ENABLED = 1
    REMOVED = 2


AGE_RANGE_CRITERIA_MAPPING = {
    "18-24": "AGE_RANGE_18_24",
    "25-34": "AGE_RANGE_25_34",
    "35-44": "AGE_RANGE_35_44",
    "45-54": "AGE_RANGE_45_54",
    "55-64": "AGE_RANGE_55_64",
    "65 or more": "AGE_RANGE_65_UP",
    "Undetermined": "AGE_RANGE_UNDETERMINED",
}
