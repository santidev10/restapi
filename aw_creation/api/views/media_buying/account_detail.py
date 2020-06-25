import hashlib
import json

from django.core.serializers.json import DjangoJSONEncoder
from django.http import Http404
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.api.serializers.analytics.account_creation_details_serializer import \
    AnalyticsAccountCreationDetailsSerializer
from aw_creation.api.serializers.media_buying.account_serializer import AccountMediaBuyingSerializer
from aw_creation.api.views.media_buying.utils import get_account_creation
from aw_creation.models import AccountCreation
from utils.api.cache import cache_method
from utils.permissions import MediaBuyingAddOnPermission


class AccountDetailAPIView(APIView):
    """
    GET: Retrieve account details

    """
    CACHE_KEY_PREFIX = "restapi.aw_creation.views.media_buying.account_detail"
    serializer_class = AnalyticsAccountCreationDetailsSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    def get(self, request, *args, **kwargs):
        pk = kwargs["pk"]
        account_creation = get_account_creation(request.user, pk)
        data = self._get_account_detail(account_creation, request)
        return Response(data=data)

    def _get_account_creation(self, request, pk):
        user = request.user
        try:
            return AccountCreation.objects.user_related(user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404

    @cache_method(timeout=1800)
    def _get_account_detail(self, account_creation, request):
        data = AccountMediaBuyingSerializer(account_creation, context=dict(request=request)).data
        return data

    def get_cache_key(self, part, options):
        data = dict(
            account_creation_id=options[0][0].id,
        )
        key_json = json.dumps(data, sort_keys=True, cls=DjangoJSONEncoder)
        key_hash = hashlib.md5(key_json.encode()).hexdigest()
        key = f"{self.CACHE_KEY_PREFIX}.{part}.{key_hash}"
        return key, key_json
