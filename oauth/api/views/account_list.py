from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated

from oauth.api.serializers import AccountSerializer
from oauth.models import Account


class GAdsAccountListAPIView(ListAPIView):
    permission_classes = (
        IsAuthenticated,
    )
    serializer_class = AccountSerializer
    queryset = Account.objects.all()

    def filter_queryset(self, queryset):
        filters = {
            "oauth_accounts__user": self.request.user
        }
        return queryset.filter(**filters)
