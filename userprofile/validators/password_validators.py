from password_strength import PasswordPolicy
from rest_framework import serializers

def min_length_password_validator(value):
    length = 8
    policy = PasswordPolicy.from_names(length=length)
    if len(policy.test(value)):
        message = ("A password is required to have at least {} character{}.") \
            .format(length, '' if length == 1 else 's')
        raise serializers.ValidationError(message)

def upper_case_password_validator(value):
    quantity = 1
    policy = PasswordPolicy.from_names(uppercase=quantity)
    if len(policy.test(value)):
        message = ("A password is required to have at least {} uppercase"
                   " character{}") \
            .format(quantity, '' if quantity == 1 else 's')
        raise serializers.ValidationError(message)

def numeric_password_validator(value):
    quantity = 1
    policy = PasswordPolicy.from_names(numbers=quantity)
    if len(policy.test(value)):
        message = ("A password is required to have at least {} number{}.") \
            .format(quantity, '' if quantity == 1 else 's')
        raise serializers.ValidationError(message)

def special_char_password_validator(value):
    quantity = 1
    policy = PasswordPolicy.from_names(special=quantity)
    if len(policy.test(value)):
        message = ("A password is required to have at least {} special"
                   " character{}") \
            .format(quantity, '' if quantity == 1 else 's')
        raise serializers.ValidationError(message)
