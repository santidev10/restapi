from distutils.util import strtobool

from rest_framework.views import APIView
from rest_framework.response import Response

from oauth.utils.view import get_campaigns
from performiq.api.serializers import IQCampaignSerializer
from performiq.api.views.utils.paginator import PerformIQPaginatior
from performiq.models import IQCampaign
import performiq.tasks.start_analysis as start_analysis
from userprofile.constants import StaticPermissions
from utils.lang import get_request_prefix


class PerformIQCampaignListCreateAPIView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.PERFORMIQ),
    )
    pagination_class = PerformIQPaginatior

    def get(self, request, *args, **kwargs):
        if strtobool(request.query_params.get("analyzed", "false")):
            data = self._get_analyzed_campaigns(request)
        else:
            data = get_campaigns(request.user)
        return Response(data=data)

    def post(self, request, *args, **kwargs):
        user = request.user
        request.data["user_id"] = user.id
        serializer = IQCampaignSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        iq_campaign = serializer.save()
        link = self._get_completion_link(iq_campaign)
        start_analysis.start_analysis_task.delay(iq_campaign.id, user.email, link)
        return Response(IQCampaignSerializer(iq_campaign).data)

    def _get_analyzed_campaigns(self, request):
        paginator = self.pagination_class()
        qs = IQCampaign.objects.filter(user=request.user)

        search = request.query_params.get("search")
        if search:
            qs = qs.filter(name__icontains=search.lower())
        page = IQCampaignSerializer(paginator.paginate_queryset(qs.order_by("-id"), request), many=True).data
        response_data = paginator._get_response_data(page)
        return response_data

    def _get_completion_link(self, iq_campaign):
        request = self.request
        prefix = get_request_prefix(request)
        host = request.get_host()
        link = f"{prefix}{host}/review/{iq_campaign.id}"
        return link
