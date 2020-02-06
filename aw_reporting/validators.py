from rest_framework.exceptions import ValidationError

from aw_reporting.models import AgeGroupConstant
from aw_reporting.models import GenderConstant


class AwReportingValidator(object):
    @staticmethod
    def validate_age_group(value, should_raise=True):
        age_group = None
        try:
            if type(value) is int:
                age_group = AgeGroupConstant.from_id(value)
            else:
                age_group = AgeGroupConstant.from_string(value)
        except AgeGroupConstant.DoesNotExist:
            if should_raise:
                raise ValidationError(f"AgeGroup {value} does not exist.")
        return age_group

    @staticmethod
    def validate_gender(value, should_raise=True):
        gender = None
        try:
            if type(value) is int:
                gender = GenderConstant.from_id(value)
            else:
                gender = GenderConstant.from_string(value)
        except GenderConstant.DoesNotExist:
            if should_raise:
                raise ValidationError(f"Gender: {value} does not exist.")
        return gender
