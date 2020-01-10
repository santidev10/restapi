from rest_framework.serializers import ModelSerializer

from aw_reporting.models import Opportunity


class OpportunitySerializer(ModelSerializer):
    class Meta:
        model = Opportunity
        fields = (
            "id",
            "name",
            "start",
        )
