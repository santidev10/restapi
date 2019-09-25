from rest_framework.serializers import CharField
from rest_framework.serializers import ModelSerializer

from ads_analyzer.models import OpportunityTargetingReport


class OpportunityTargetReportPayloadSerializer(ModelSerializer):
    opportunity_id = CharField(max_length=20, required=True)

    class Meta:
        model = OpportunityTargetingReport
        fields = (
            "opportunity_id",
            "date_from",
            "date_to",
        )
