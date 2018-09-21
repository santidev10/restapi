from django.contrib.auth.models import Group
from rest_framework.serializers import CharField
from rest_framework.serializers import Serializer


class GroupSerializer(Serializer):
    name = CharField(read_only=True)

    class Meta:
        model = Group
        fields = (
            'name',
        )
