from django.http import Http404
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.models import Account
from userprofile.constants import StaticPermissions


class AccountCreationByAccountAPIView(APIView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MANAGED_SERVICE),)

    def get(self, request, account_id):
        user = request.user
        accounts_queryset = Account.objects.all()
        if not user.has_permission(StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS):
            visible_accounts = user.get_visible_accounts_list()
            accounts_queryset = accounts_queryset \
                .filter(id__in=visible_accounts)
        try:
            account = accounts_queryset.get(id=account_id)
        except Account.DoesNotExist:
            raise Http404
        return Response(dict(id=account.account_creation.id))
