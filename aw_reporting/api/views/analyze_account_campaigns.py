from rest_framework.generics import ListAPIView

from aw_reporting.api.serializers import CampaignListSerializer
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import Campaign


@demo_view_decorator
class AnalyzeAccountCampaignsListApiView(ListAPIView):
    """
    Return a list of the account's campaigns/ad-groups
    We use it to build filters
    """
    serializer_class = CampaignListSerializer

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        return Campaign.objects.filter(account_id=pk)
