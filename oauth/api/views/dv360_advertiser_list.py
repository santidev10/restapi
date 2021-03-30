from rest_framework.generics import ListAPIView

from oauth.api.serializers import AdvertiserSerializer
from oauth.models import DV360Advertiser
from rest_framework.permissions import IsAuthenticated


class OAuthDV360AdvertiserListAPIView(ListAPIView):
    permission_classes = (
        IsAuthenticated,
    )
    serializer_class = AdvertiserSerializer

    def get_queryset(self):
        return DV360Advertiser.objects.filter(oauth_accounts__user=self.request.user)
