from rest_framework.fields import CharField
from rest_framework.fields import DateField
from rest_framework.serializers import ModelSerializer

from ads_analyzer.models import OpportunityTargetingReport


class OpportunityTargetReportPayloadSerializer(ModelSerializer):
    date_from = DateField(allow_null=True, required=False)
    date_to = DateField(allow_null=True, required=False)

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


class OpportunityTargetReportModelSerializer(ModelSerializer):
    download_link = CharField(source="external_link")

    class Meta:
        model = OpportunityTargetingReport
        fields = (
            "id",
            "opportunity",
            "date_from",
            "date_to",
            "created_at",
            "download_link",
        )
