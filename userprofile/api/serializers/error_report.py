from rest_framework.serializers import CharField
from rest_framework.serializers import EmailField
from rest_framework.serializers import Serializer


class ErrorReportSerializer(Serializer):
    email = EmailField(max_length=255)
    message = CharField(required=True)
