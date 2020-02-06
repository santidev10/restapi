from django.db import models


class AgeGroupConstant(models.Model):
    ID_CHOICES = [
        (0, "Age range undetermined"),
        (1, "Age 18 - 24"),
        (2, "Age 25 - 34"),
        (3, "Age 35 - 44"),
        (4, "Age 45 - 54"),
        (5, "Age 55 - 64"),
        (6, "Age 65 - up"),
    ]
    AGE_GROUP_CHOICES = [
        ("AGE_RANGE_UNDETERMINED", "Age range undetermined"),
        ("AGE_RANGE_18_24", "Age 18 - 24"),
        ("AGE_RANGE_25_34", "Age 25 - 34"),
        ("AGE_RANGE_35_44", "Age 35 - 44"),
        ("AGE_RANGE_45_54", "Age 45 - 54"),
        ("AGE_RANGE_55_64", "Age 55 - 64"),
        ("AGE_RANGE_65_UP", "Age 65 - up"),
    ]

    to_str = dict(AGE_GROUP_CHOICES)
    to_id = {val: key for key, val in to_str.items()}

    id = models.IntegerField(primary_key=True, choices=ID_CHOICES)
    age_group = models.CharField(choices=AGE_GROUP_CHOICES, max_length=25)

    @staticmethod
    def from_string(value):
        age_group = AgeGroupConstant.objects.get(age_group=value)
        return age_group

    @staticmethod
    def from_id(value):
        age_group = AgeGroupConstant.objects.get(id=value)
        return age_group


class GenderConstant(models.Model):
    ID_CHOICES = [
        (0, "Undetermined"),
        (1, "Female"),
        (2, "Male"),
    ]
    GENDER_CHOICES = [
        ("UNDETERMINED", "Undetermined"),
        ("FEMALE", "Female"),
        ("MALE", "Male"),
    ]
    to_str = dict(ID_CHOICES)
    to_id = {val: key for key, val in to_str.items()}

    id = models.IntegerField(primary_key=True, choices=ID_CHOICES)
    gender = models.CharField(max_length=15, choices=GENDER_CHOICES)

    @staticmethod
    def from_string(value):
        gender = GenderConstant.objects.get(gender=value)
        return gender

    @staticmethod
    def from_id(value):
        gender = GenderConstant.objects.get(gender=value)
        return gender

