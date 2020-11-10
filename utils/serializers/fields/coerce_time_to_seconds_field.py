from rest_framework import serializers
from rest_framework.serializers import empty
from rest_framework.exceptions import ValidationError


class CoerceTimeToSecondsField(serializers.Field):
    """
    Require either an int or string in the formats: hh:mm:ss, or mm:ss
    """
    def run_validation(self, data=None):
        # if the field is not passed
        if data is empty:
            if self.required:
                raise ValidationError("This field is required")
            else:
                return None
        # if the value is null
        if data is None or data == "":
            if self.allow_null:
                return None
            else:
                raise ValidationError("This field cannot be null")
        # validate type and format
        if not isinstance(data, str) and not isinstance(data, int):
            raise ValidationError(f"An integer, or string in the formats: 'hh:mm:ss', 'mm:ss' or 'ss' is required")
        if isinstance(data, int):
            return data
        try:
            coerced_seconds = int(data)
        except ValueError:
            split = data.split(":")
            if len(split) not in [2, 3]:
                raise ValidationError(f"The string must follow the format: 'hh:mm:ss', 'mm:ss' or 'ss'")
            try:
                split = list(map(int, split))
            except ValueError:
                raise ValidationError(f"each time component must be a valid integer")
            if len(split) == 2:
                list(map(self.validate_ceiling, split))
                minutes, seconds = split
                return minutes * 60 + seconds
            list(map(self.validate_ceiling, split[1:]))
            hours, minutes, seconds = split
            return hours * 3600 + minutes * 60 + seconds
        return coerced_seconds

    @ staticmethod
    def validate_ceiling(value: int, ceiling=60):
        if value > ceiling:
            raise ValidationError(f"The time component: '{value}' must be less than or equal to {ceiling}")
        return value


