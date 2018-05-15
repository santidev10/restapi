from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.serializers import ModelSerializer
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.views import APIView
from aw_reporting.demo.models import DemoAccount
from aw_reporting.models import Account
from aw_reporting.settings import AdwordsAccountSettings
from utils.cache import cache_reset
from utils.cache import cached_view_decorator as cached_view


class AdWordsTopManagerSerializer(ModelSerializer):
    class Meta:
        model = Account
        fields = ("id", "name")


class VisibleAccountsApiView(APIView):
    """
    Visible account list view/edit
    """
    permission_classes = (IsAdminUser,)
    queryset = Account.objects.filter(managers__isnull=False).order_by('name')
    serializer_class = AdWordsTopManagerSerializer

    def get(self, request):
        data = self.serializer_class(self.queryset.all(), many=True).data
        settings = request.user.aw_settings

        if not settings:
            # empty user aw settings
            return Response(data=[])

        visible_ids = settings.get('visible_accounts')
        types_settings = settings.get('hidden_campaign_types')
        campaign_types = AdwordsAccountSettings.CAMPAIGN_TYPES

        for ac_info in data:
            account_id = ac_info['id']
            ac_info['visible'] = account_id in visible_ids

            hidden_types = types_settings.get(account_id, [])
            ac_info['campaign_types_visibility'] = [
                dict(
                    id=ct,
                    name=ct.capitalize().replace("_", "-"),
                    visible=ct not in hidden_types,
                )
                for ct in campaign_types
                ]

        demo = DemoAccount()
        demo_hidden_types = types_settings.get(demo.id, [])
        data.insert(
            0,
            dict(
                id=demo.id,
                name=demo.name,
                visible=demo.visible,
                campaign_types_visibility=[
                    dict(
                        id=ct,
                        name=ct.capitalize().replace("_", "-"),
                        visible=ct not in demo_hidden_types,
                    )
                    for ct in campaign_types
                    ],
            )
        )
        return Response(data=data)

    @cached_view
    def put(self, request):
        settings_obj = request.user.aw_settings
        if 'accounts' in request.data:
            accounts = request.data.get('accounts')

            visible_accounts = set(settings_obj.get('visible_accounts'))
            hidden_types = settings_obj.get('hidden_campaign_types')

            for account in accounts:
                # account visibility
                uid = account['id']
                if uid == "demo":
                    settings_obj.update(
                        demo_account_visible=account['visible'])
                else:
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
            request.user.save()

        cache_reset()
        return self.get(request)


class UserAWSettingsApiView(APIView):
    """
    Visible account list view/edit
    """
    permission_classes = tuple()

    def get(self, request):
        user_aw_settings = request.user.aw_settings
        return Response(data=user_aw_settings)

    @cached_view
    def put(self, request):
        # get user settings
        user_aw_settings = request.user.aw_settings

        # check for valid data in request body
        keys_to_update = request.data.keys() & set(AdwordsAccountSettings.AVAILABLE_KEYS)

        # update user aw settings and save changes
        for key in keys_to_update:
            user_aw_settings[key] = request.data[key]

        request.user.save()
        return Response(data=user_aw_settings,
                        status=HTTP_202_ACCEPTED)
