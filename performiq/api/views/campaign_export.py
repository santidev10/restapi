from rest_framework.views import APIView
from rest_framework.response import Response

from performiq.models import IQCampaign
from performiq.models.constants import EXPORT_RESULTS_KEYS
from performiq.utils.s3_exporter import PerformS3Exporter
from utils.views import get_object


class PerformIQCampaignExportAPIView(APIView):
    def get(self, request, *args, **kwargs):
        export_type = int(request.query_params.get("type", 0))
        export_key = EXPORT_RESULTS_KEYS.RECOMMENDED_EXPORT_FILENAME if export_type == 0 \
            else EXPORT_RESULTS_KEYS.WASTAGE_EXPORT_FILENAME
        iq_campaign = get_object(IQCampaign, id=kwargs["pk"])
        s3 = PerformS3Exporter()
        download_url = s3.generate_temporary_url(iq_campaign.results["exports"][export_key])
        response = {"download_url": download_url}
        return Response(response)
