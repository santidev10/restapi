from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated

from oauth.api.serializers import LineItemSerializer
from oauth.models import LineItem


class LineItemListAPIView(ListAPIView):
    permission_classes = (IsAuthenticated,)
    serializer_class = LineItemSerializer
    queryset = LineItem.objects.all()

    def filter_queryset(self, queryset):
        filters = {
            "insertion_order__campaign__advertiser__oauth_accounts__user": self.request.user
        }
        parent_id = self.request.query_params.get("parent_id")
        if parent_id:
            filters["insertion_order_id"] = parent_id
        return queryset.filter(**filters)
