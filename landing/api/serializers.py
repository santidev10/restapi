"""
Feedback api serializers module
"""
import phonenumbers

from rest_framework.serializers import Serializer, ValidationError
from rest_framework.fields import CharField, EmailField


class ContactMessageSendSerializer(Serializer):
    """
    Serializer for feedback send process
    """
    name = CharField(max_length=255, required=True)
    email = EmailField(max_length=255, required=True)
    subject = CharField(max_length=100, required=True)
    message = CharField(required=True)
    company = CharField(max_length=100, required=True)
    phone = CharField(max_length=30, required=True)

    def validate_phone(self, value):
        try:
            phonenumbers.parse(value)
        except phonenumbers.NumberParseException:
            raise ValidationError("Incorrect phone number format")
        return value
