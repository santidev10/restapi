from django.db.models import Sum
from rest_framework.response import Response
from rest_framework.status import HTTP_405_METHOD_NOT_ALLOWED

from .account_creation_list import AccountCreationListApiView


class PerformanceTargetingListAPIView(AccountCreationListApiView):
    def get_queryset(self, **filters):
        queryset = super(PerformanceTargetingListAPIView, self).get_queryset(
            **filters)
        queryset = queryset.annotate(
            sum_cost=Sum("account__campaigns__cost"),
        ).filter(sum_cost__gt=0)
        return queryset

    def post(self, *a, **_):
        return Response(status=HTTP_405_METHOD_NOT_ALLOWED)
