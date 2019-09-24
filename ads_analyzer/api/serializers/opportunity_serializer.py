from rest_framework.serializers import CharField
from rest_framework.serializers import ModelSerializer

from aw_reporting.models import Opportunity


class OpportunitySerializer(ModelSerializer):
    id = CharField()
    name = CharField()

    class Meta:
        model = Opportunity
        fields = (
            "id",
            "name",
        )
