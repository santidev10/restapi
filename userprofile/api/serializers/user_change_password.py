from rest_framework.serializers import CharField
from rest_framework.serializers import Serializer

from userprofile.validators import password_validators

class UserChangePasswordSerializer(Serializer):
    """
    Serializer for changing user's password.
    """
    new_password = CharField(required=True, validators=[
        password_validators.upper_case_password_validator,
        password_validators.min_length_password_validator,
        password_validators.special_char_password_validator,
        password_validators.numeric_password_validator
    ])
    old_password = CharField(required=True)
