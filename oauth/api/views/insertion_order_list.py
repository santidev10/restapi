from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated

from oauth.api.serializers import InsertionOrderSerializer
from oauth.models import InsertionOrder


class InsertionOrderListAPIView(ListAPIView):
    permission_classes = (IsAuthenticated,)
    serializer_class = InsertionOrderSerializer
    queryset = InsertionOrder.objects.all()

    def filter_queryset(self, queryset):
        filters = {
            "campaign__advertiser__oauth_accounts__user": self.request.user
        }
        parent_id = self.request.query_params.get("parent_id")
        if parent_id:
            filters["campaign_id"] = parent_id
        return queryset.filter(**filters)
