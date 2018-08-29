from django.http import Http404
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.models import Account
from userprofile.models import UserSettingsKey


class AccountCreationByAccountAPIView(APIView):
    def get(self, request, account_id):
        user = request.user
        user_settings = user.get_aw_settings()
        accounts_queryset = Account.objects.all()
        if not user_settings.get(UserSettingsKey.VISIBLE_ALL_ACCOUNTS):
            visible_accounts = user_settings.get(
                UserSettingsKey.VISIBLE_ACCOUNTS)
            accounts_queryset = accounts_queryset \
                .filter(id__in=visible_accounts)
        try:
            account = accounts_queryset.get(id=account_id)
        except Account.DoesNotExist:
            raise Http404
        return Response(dict(id=account.account_creation.id))
