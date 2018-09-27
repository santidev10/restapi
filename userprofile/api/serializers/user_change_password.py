from rest_framework.serializers import CharField
from rest_framework.serializers import Serializer


class UserChangePasswordSerializer(Serializer):
    """
    Serializer for changing user's password.
    """
    new_password = CharField(required=True)
    old_password = CharField(required=True)
