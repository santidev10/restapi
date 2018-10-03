from typing import Type

from rest_framework.exceptions import ValidationError

from utils.lang import ExtendedEnum


def extended_enum(enum_cls: Type[ExtendedEnum]):
    def enum_validator(value):
        if not enum_cls.has_value(value):
            raise ValidationError("Invalid value should be one of '{}'".format(", ".join(enum_cls.values())))

    return enum_validator
