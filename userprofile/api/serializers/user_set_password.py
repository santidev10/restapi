from rest_framework.serializers import CharField
from rest_framework.serializers import Serializer


class UserSetPasswordSerializer(Serializer):
    """
    Serializer for password set endpoint.
    """
    new_password = CharField(required=True)
    email = CharField(required=True)
    token = CharField(required=True)
