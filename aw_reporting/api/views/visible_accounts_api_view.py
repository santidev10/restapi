from django.conf import settings
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.serializers import ModelSerializer
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_reporting.models import Account, campaign_type_str
from aw_reporting.settings import AdwordsAccountSettings
from userprofile.constants import UserSettingsKey
from userprofile.models import UserProfile
from utils.cache import cache_reset
from utils.cache import cached_view_decorator as cached_view


class AdWordsTopManagerSerializer(ModelSerializer):
    class Meta:
        model = Account
        fields = ("id", "name")


class GetUserMixin:
    def get_user_by_id(self, user_id):
        try:
            user = UserProfile.objects.get(id=user_id)
        except UserProfile.DoesNotExist:
            user = None

        return user


class VisibleAccountsApiView(APIView, GetUserMixin):
    """
    Visible account list view/edit
    """
    permission_classes = (IsAdminUser,)
    queryset = Account.objects.filter(managers__isnull=False).order_by('name')
    serializer_class = AdWordsTopManagerSerializer

    def get(self, request):
        chf_account_id = settings.CHANNEL_FACTORY_ACCOUNT_ID
        data = self.serializer_class(
            self.queryset.filter(managers__id=chf_account_id).distinct(),
            many=True
        ).data
        user_id = self.request.query_params.get('user_id')
        user = self.get_user_by_id(user_id)
        if user is None:
            return Response(status=HTTP_404_NOT_FOUND)
        aw_settings = user.get_aw_settings()
        visible_ids = aw_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
        types_settings = aw_settings.get(UserSettingsKey.HIDDEN_CAMPAIGN_TYPES)
        campaign_types = AdwordsAccountSettings.CAMPAIGN_TYPES

        for ac_info in data:
            account_id = ac_info['id']
            ac_info['visible'] = account_id in visible_ids

            hidden_types = types_settings.get(account_id, [])
            ac_info['campaign_types_visibility'] = [
                dict(
                    id=ct,
                    name=campaign_type_str(ct),
                    visible=ct not in hidden_types,
                )
                for ct in campaign_types
                ]

        return Response(data=data)

    @cached_view
    def put(self, request):
        user_id = self.request.query_params.get('user_id')
        user = self.get_user_by_id(user_id)
        if user is None:
            return Response(status=HTTP_404_NOT_FOUND)
        settings_obj = user.get_aw_settings()
        if 'accounts' in request.data:
            accounts = request.data.get('accounts')

            visible_accounts = set(settings_obj.get(UserSettingsKey.VISIBLE_ACCOUNTS))
            hidden_types = settings_obj.get(UserSettingsKey.HIDDEN_CAMPAIGN_TYPES)

            for account in accounts:
                # account visibility
                uid = account['id']
                if account['visible']:
                    visible_accounts |= {uid}
                else:
                    visible_accounts -= {uid}

                # campaign visibility
                if 'campaign_types_visibility' in account:
                    hidden_types[uid] = [
                        k for k, v in
                        account['campaign_types_visibility'].items()
                        if not v
                        ]

            update = dict(visible_accounts=list(sorted(visible_accounts)),
                          hidden_campaign_types=hidden_types)
            settings_obj.update(update)
            user.aw_settings = settings_obj
            user.save()

        cache_reset()
        return self.get(request)


class UserAWSettingsApiView(APIView, GetUserMixin):
    """
    Visible account list view/edit
    """
    permission_classes = tuple()

    def get(self, request):
        user_id = self.request.query_params.get('user_id')
        user = self.get_user_by_id(user_id)
        if user is None:
            return Response(status=HTTP_404_NOT_FOUND)
        user_aw_settings = user.get_aw_settings()
        return Response(data=user_aw_settings)

    @cached_view
    def put(self, request):
        # get user settings
        user_id = self.request.query_params.get('user_id')
        user = self.get_user_by_id(user_id)
        if user is None:
            return Response(status=HTTP_404_NOT_FOUND)
        user_aw_settings = user.get_aw_settings()

        # check for valid data in request body
        keys_to_update = request.data.keys() & set(AdwordsAccountSettings.AVAILABLE_KEYS)

        # update user aw settings and save changes
        for key in keys_to_update:
            user_aw_settings[key] = request.data[key]

        user.aw_settings = user_aw_settings
        user.save()
        return Response(data=user_aw_settings,
                        status=HTTP_202_ACCEPTED)
