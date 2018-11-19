from rest_framework.serializers import CharField
from rest_framework.serializers import EmailField
from rest_framework.serializers import Serializer


class ContactFormSerializer(Serializer):
    """
    Serializer for contact form fields
    """
    first_name = CharField(required=True, max_length=255)
    last_name = CharField(required=True, max_length=255)
    email = EmailField(required=True, max_length=255)
    country = CharField(required=True, max_length=255)
    company = CharField(required=True, max_length=255)
    message = CharField(
        required=False,
        max_length=255,
        default="",
        allow_blank=True
    )
