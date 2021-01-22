from rest_framework.exceptions import ValidationError
from rest_framework.views import APIView
from rest_framework.response import Response

from performiq.models import IQCampaign
from performiq.models.constants import EXPORT_RESULTS_KEYS
from performiq.utils.s3_exporter import PerformS3Exporter
from userprofile.constants import StaticPermissions
from utils.views import get_object


class PerformIQCampaignExportAPIView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.PERFORMIQ),
    )

    EXPORT_TYPES = {
        0: EXPORT_RESULTS_KEYS.RECOMMENDED_EXPORT_FILENAME,
        1: EXPORT_RESULTS_KEYS.WASTAGE_EXPORT_FILENAME,
    }

    def get(self, request, *args, **kwargs):
        export_type = int(request.query_params.get("type", 0))
        try:
            export_key = self.EXPORT_TYPES[export_type]
        except KeyError:
            raise ValidationError(f"Invalid type: {export_type}. type must be 0 = recommended or 1 = wastage")
        iq_campaign = get_object(IQCampaign, id=kwargs["pk"])
        s3 = PerformS3Exporter()
        download_url = s3.generate_temporary_url(iq_campaign.results["exports"][export_key])
        response = {"download_url": download_url}
        return Response(response)
