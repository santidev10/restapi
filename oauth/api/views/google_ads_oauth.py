from googleads.errors import GoogleAdsServerFault
from oauth2client.client import HttpAccessTokenRefreshError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from suds import WebFault

from .base_google_oauth import BaseGoogleOAuthAPIView
from oauth.constants import OAuthType
from oauth.tasks.google_ads_update import google_ads_update_task
from performiq.api.serializers.aw_auth_serializer import AWAuthSerializer
from performiq.utils.adwords_report import get_accounts


class GoogleAdsOAuthAPIView(BaseGoogleOAuthAPIView):
    """
        API View for Granting AdWords OAuth Access to PerformIQ
        GET method gives a URL to go and grant access to our app
        then send the code you will get in the query in POST request

        POST body example:
        {"code": "<INSERT YOUR CODE HERE>"}

        success POST response example:
        {"email": "your@email.com",
        "mcc_accounts": [{"id": 1234, "name": "Test Acc", "currency_code": "UAH",
         "timezone": "Ukraine/Kiev"}]
        }
        """
    permission_classes = ()
    no_mcc_error = "MCC account wasn't found. Please check that you " \
                   "really have access to at least one."

    @property
    def oauth_type(self):
        return OAuthType.GOOGLE_ADS.value

    def handler(self, oauth_account):
        # Get Name of First MCC Account
        is_err = False
        try:
            mcc_accounts, cid_accounts = get_accounts(oauth_account.refresh_token)
        except WebFault as e:
            is_err = True
            fault_string = e.fault.faultstring
            if "AuthenticationError.NOT_ADS_USER" in fault_string:
                fault_string = "AdWords account does not exist"
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error=fault_string))
        except HttpAccessTokenRefreshError as e:
            is_err = True
            ex_token_error = "Token has been expired or revoked"
            if ex_token_error in str(e):
                return Response(status=HTTP_400_BAD_REQUEST,
                                data=dict(error=ex_token_error))
        except GoogleAdsServerFault as e:
            is_err = True
            error_strings = []
            for error in e.errors:
                error_strings.append(error.errorString)
                # no assigned cids/not an ads user
                if error.errorString in ["AuthenticationError.CUSTOMER_NOT_FOUND", "AuthenticationError.NOT_ADS_USER"]:
                    return Response(status=HTTP_400_BAD_REQUEST, data=dict(err="You do not have a Google Ads account."))
                if error.errorString == "AuthenticationError.OAUTH_TOKEN_INVALID":
                    return Response(status=HTTP_400_BAD_REQUEST, data=dict(error="Invalid OAuth token!"))
            return Response(status=HTTP_400_BAD_REQUEST, data={f"GoogleAds ServerFault: {', '.join(error_strings)}"})
        else:
            if mcc_accounts:
                first = mcc_accounts[0]
                oauth_account.name = first["descriptiveName"]
                oauth_account.save(update_fields=["name"])

            if mcc_accounts or cid_accounts:
                response = AWAuthSerializer(oauth_account).data
                status = HTTP_200_OK
            else:
                response = "You have no accounts to sync."
                status = HTTP_400_BAD_REQUEST
            google_ads_update_task.delay([oauth_account.id])
            return Response(data=response, status=status)
        finally:
            if is_err:
                oauth_account.synced = True
                oauth_account.save(update_fields=["synced"])
