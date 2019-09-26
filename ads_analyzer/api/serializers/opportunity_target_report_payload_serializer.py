from rest_framework.serializers import ModelSerializer

from ads_analyzer.models import OpportunityTargetingReport


class OpportunityTargetReportPayloadSerializer(ModelSerializer):
    def create(self, validated_data):
        report, _ = self.Meta.model.objects.update_or_create(**validated_data)
        return report

    class Meta:
        model = OpportunityTargetingReport
        fields = (
            "opportunity",
            "date_from",
            "date_to",
        )
