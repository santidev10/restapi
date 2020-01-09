from django.contrib.auth import get_user_model

from rest_framework.fields import CharField
from rest_framework.fields import DateField
from rest_framework.serializers import ModelSerializer

from ads_analyzer.models import OpportunityTargetingReport
from ads_analyzer.reports.opportunity_targeting_report.s3_exporter import OpportunityTargetingReportS3Exporter
from utils.api.fields import CharFieldListBased


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


class ReportDownloadLink(CharField):
    def to_representation(self, s3_file_key):
        s3_link = OpportunityTargetingReportS3Exporter.generate_temporary_url(s3_file_key)
        return super().to_representation(s3_link)


class RecipientsListField(CharFieldListBased):
    def to_representation(self, recipients):
        data = [str(recipient) for recipient in recipients.all()]
        return super(RecipientsListField, self).to_representation(data)


class OpportunityTargetReportRecipientsSerializer(ModelSerializer):

    class Meta:
        model = get_user_model()
        fields = (
            "id",
            "first_name",
            "last_name"
        )


class OpportunityTargetReportModelSerializer(ModelSerializer):
    download_link = ReportDownloadLink(source="s3_file_key")
    opportunity = CharField(source="opportunity.name")
    recipients = RecipientsListField()

    class Meta:
        model = OpportunityTargetingReport
        fields = (
            "id",
            "opportunity",
            "opportunity_id",
            "date_from",
            "date_to",
            "created_at",
            "download_link",
            "status",
            "recipients"
        )
