from rest_framework.fields import CharField
from rest_framework.serializers import ModelSerializer

from ads_analyzer.models import OpportunityTargetingReport


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
