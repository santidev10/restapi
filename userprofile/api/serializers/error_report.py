from rest_framework.serializers import CharField
from rest_framework.serializers import EmailField
from rest_framework.serializers import Serializer


class ErrorReportSerializer(Serializer):
    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError

    email = EmailField(max_length=255)
    message = CharField(required=True)
