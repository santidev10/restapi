"""
Feedback api serializers module
"""
from rest_framework.serializers import Serializer
from rest_framework.fields import CharField, EmailField


class ContactMessageSendSerializer(Serializer):
    """
    Serializer for feedback send process
    """
    name = CharField(max_length=255, required=True)
    email = EmailField(max_length=255, required=True)
    subject = CharField(max_length=100, required=True)
    message = CharField(required=True)
    company = CharField(max_length=100, required=False)
    phone = CharField(max_length=30, required=False)
