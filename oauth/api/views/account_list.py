from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated

from oauth.api.serializers import AccountSerializer
from oauth.models import Account


class GAdsAccountListAPIView(ListAPIView):
    permission_classes = (
        IsAuthenticated,
    )
    serializer_class = AccountSerializer

    def get_queryset(self):
        filters = {
            "oauth_accounts__user": self.request.user
        }
        return Account.objects.filter(**filters)
