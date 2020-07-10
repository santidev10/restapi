from django.db.models import F
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

from aw_creation.models import AccountCreation


class AwCreationChangedAccountsListAPIView(GenericAPIView):
    permission_classes = tuple()

    @staticmethod
    def get(*_, **kwargs):
        manager_id = kwargs.get("manager_id")
        ids = AccountCreation.objects.filter(
            account__managers__id=manager_id,
            account_id__isnull=False,
            is_managed=True,
            is_approved=True,
        ).exclude(
            sync_at__gte=F("updated_at"),
        ).values_list(
            "account_id", flat=True
        ).order_by("account_id").distinct()
        return Response(data=ids)
