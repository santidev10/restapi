from distutils.util import strtobool

from rest_framework.views import APIView
from rest_framework.response import Response

from .utils.get_campaigns import get_campaigns
from performiq.api.serializers import IQCampaignSerializer
from performiq.api.views.utils.paginator import PerformIQPaginatior
from performiq.models import IQCampaign


class PerformIQCampaignListCreateAPIView(APIView):
    pagination_class = PerformIQPaginatior

    def get(self, request, *args, **kwargs):
        if strtobool(request.query_params.get("analyzed", "false")):
            data = self._get_analyzed_campaigns(request)
        else:
            data = get_campaigns(request.user)
        return Response(data=data)

    def post(self, request, *args, **kwargs):
        request.data["user_id"] = request.user.id
        serializer = IQCampaignSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_params = serializer.validated_data
        iq_campaign = serializer.save()
        validated_params["id"] = iq_campaign.id
        return Response(validated_params)

    def _get_analyzed_campaigns(self, request):
        paginator = self.pagination_class()
        qs = IQCampaign.objects.filter(campaign__account__oauth_account__user=request.user) \
             | IQCampaign.objects.filter(campaign__advertiser__oauth_accounts__user=request.user) \
             | IQCampaign.objects.filter(campaign__isnull=True, user=request.user)

        search = request.query_params.get("search")
        sort = request.query_params.get("sort")
        if search is not None:
            qs = qs.filter(name__icontains=search)
        if sort == "-":
            qs.order_by("-name")
        elif sort == "+":
            qs.order_by("name")

        page = IQCampaignSerializer(paginator.paginate_queryset(qs.order_by("id"), request), many=True).data
        response_data = paginator._get_response_data(page)
        return response_data
