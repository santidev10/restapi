from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated

from oauth.api.serializers import CampaignSerializer
from oauth.constants import OAuthType
from oauth.models import Campaign
from oauth.utils.view import validate_oath_type


class OAuthCampaignListAPIView(ListAPIView):
    permission_classes = (
        IsAuthenticated,

    )
    serializer_class = CampaignSerializer

    def get_queryset(self):
        request = self.request
        query_params = self.request.query_params
        oauth_type = validate_oath_type(request)

        parent_id = query_params.get("parent_id")
        filter_config = self._get_filter_config(oauth_type)
        filters = {
            filter_config["user"]: request.user,
        }
        if parent_id:
            filters[filter_config["parent"]] = parent_id
        qs = Campaign.objects.filter(**filters)
        return qs

    def _get_filter_config(self, oauth_type):
        config = {
            OAuthType.GOOGLE_ADS.value: {
                "parent": "account_id",
                "user": "account__oauth_accounts__user",
            },
            OAuthType.DV360.value: {
                "parent": "advertiser_id",
                "user": "advertiser__oauth_accounts__user",
            }
        }
        return config[oauth_type]
