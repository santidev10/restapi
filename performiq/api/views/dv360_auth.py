from rest_framework.response import Response

from performiq.api.views.utils.performiq_permission import PerformIQPermission
from performiq.oauth_utils import load_client_settings
from performiq.tasks.dv360.sync_dv_records import sync_dv_partners
from oauth.api.views import BaseDV360AuthApiView


class DV360AuthApiView(BaseDV360AuthApiView):
    permission_classes = (PerformIQPermission,)

    def handler(self, oauth_account):
        sync_dv_partners.delay(oauth_account_ids=[oauth_account.id], sync_advertisers=True)
        return Response(status=204)

    def get_client_settings(self, *args, **kwargs):
        client_settings = load_client_settings()
        return client_settings
