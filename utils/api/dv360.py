import requests
from urllib.parse import urlencode
from performiq.models import OAuthAccount

class DV360APIClient:

    SERVICE_ENDPOINT = "https://displayvideo.googleapis.com"
    ENDPOINT_VERSION = "v1"

    def __init__(self, access_token=None):
        # TODO remove
        awong = OAuthAccount.objects.get(id=3)
        access_token = awong.token

        if access_token:
            self.access_token = access_token
        pass

    def get_base_url(self):
        return f"{self.SERVICE_ENDPOINT}/{self.ENDPOINT_VERSION}"

    def list_campaigns(self, advertiser_id, access_token=None):
        """
        user has many partners
        partner has many advertisers
        advertiser has many campaigns
        """
        params = {
            "access_token": access_token if access_token else self.access_token,
            # "advertiserId": advertiser_id,
        }
        url = f"{self.get_base_url()}/advertisers/{advertiser_id}/campaigns"
        return requests.get(url, params=params)

    def list_advertisers(self, partner_id, access_token=None):
        """
        user has many partners
        partner has many advertisers
        """
        params = {
            "access_token": access_token if access_token else self.access_token,
            "partnerId": partner_id,
        }
        url = f"{self.get_base_url()}/advertisers"
        response = requests.get(url, params=params)

        j = response.json()
        return response

    def list_partners(self, access_token=None):
        """
        user has many partners
        """
        params = {
            "access_token": access_token if access_token else self.access_token
        }
        url = f"{self.get_base_url()}/partners"
        response = requests.get(url, params=params)

        j = response.json()
        return response


    # TODO remove
    def get_first_item_id(self, item_type: str, res: requests.Response)-> str:
        data = res.json()
        first_item = data[f"{item_type}s"][0]
        return first_item[f"{item_type}Id"]

    # TODO remove
    def get_partner_id(self, partners_res):
        j = partners_res.json()
        partner = j['partners'][0]
        return partner['partnerId']

    # TODO remove
    def get_first_advertiser_id(self, advertisers_res):
        j = advertisers_res.json()
        advertiser = j["advertisers"][0]
        return advertiser['advertiserId']