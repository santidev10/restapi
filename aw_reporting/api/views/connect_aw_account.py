from django.db import transaction
from oauth2client import client
from oauth2client.client import HttpAccessTokenRefreshError
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView
from suds import WebFault

from aw_reporting.adwords_api import get_customers
from aw_reporting.adwords_api import load_web_app_settings
from aw_reporting.api.serializers import AWAccountConnectionRelationsSerializer
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.tasks import upload_initial_aw_data
from aw_reporting.utils import get_google_access_token_info
from userprofile.permissions import PermissionGroupNames


class ConnectAWAccountApiView(APIView):
    """
    The view allows to connect user's AdWords account
    GET method gives an URL to go and grant access to our app
    then send the code you will get in the query in POST request

    POST body example:
    {"code": "<INSERT YOUR CODE HERE>"}

    success POST response example:
    {"email": "your@email.com",
    "mcc_accounts": [{"id": 1234, "name": "Test Acc", "currency_code": "UAH",
     "timezone": "Ukraine/Kiev"}]
    }
    """

    scopes = (
        "https://www.googleapis.com/auth/adwords",
        "https://www.googleapis.com/auth/userinfo.email",
    )
    lost_perm_error = "You have already provided access to your accounts" \
                      " but we've lost it. Please, visit " \
                      "https://myaccount.google.com/permissions and " \
                      "revoke our application's permission " \
                      "then try again"
    no_mcc_error = "MCC account wasn't found. Please check that you " \
                   "really have access to at least one."

    # first step
    def get(self, *args, **kwargs):
        redirect_url = self.request.query_params.get("redirect_url")
        if not redirect_url:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required query param: 'redirect_url'")
            )

        flow = self.get_flow(redirect_url)
        authorize_url = flow.step1_get_authorize_url()
        return Response(dict(authorize_url=authorize_url))

    # second step
    def post(self, request, *args, **kwargs):
        # get refresh token
        redirect_url = self.request.query_params.get("redirect_url")
        if not redirect_url:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required query param: 'redirect_url'")
            )

        code = request.data.get("code")
        if not code:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Required: 'code'")
            )

        flow = self.get_flow(redirect_url)
        try:
            credential = flow.step2_exchange(code)
        except client.FlowExchangeError as e:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="Authentication has failed: %s" % e)
            )
        else:
            token_info = get_google_access_token_info(
                credential.access_token)
            if "email" not in token_info:
                return Response(status=HTTP_400_BAD_REQUEST,
                                data=token_info)

            refresh_token = credential.refresh_token
            try:
                connection = AWConnection.objects.get(
                    email=token_info["email"]
                )
            except AWConnection.DoesNotExist:
                if refresh_token:
                    connection = AWConnection.objects.create(
                        email=token_info["email"],
                        refresh_token=refresh_token,
                    )
                else:
                    return Response(
                        data=dict(error=self.lost_perm_error),
                        status=HTTP_400_BAD_REQUEST,
                    )
            else:
                # update token
                if refresh_token and \
                        connection.refresh_token != refresh_token:
                    connection.revoked_access = False
                    connection.refresh_token = refresh_token
                    connection.save()

            try:
                AWConnectionToUserRelation.objects.get(
                    user=self.request.user,
                    connection=connection,
                )
            except AWConnectionToUserRelation.DoesNotExist:
                pass
            else:
                return Response(
                    status=HTTP_400_BAD_REQUEST,
                    data=dict(error="You have already linked this account")
                )

            # -- end of get refresh token
            # save mcc accounts
            try:
                customers = get_customers(
                    connection.refresh_token,
                    **load_web_app_settings()
                )
            except WebFault as e:
                fault_string = e.fault.faultstring
                if "AuthenticationError.NOT_ADS_USER" in fault_string:
                    fault_string = "AdWords account does not exist"
                return Response(status=HTTP_400_BAD_REQUEST,
                                data=dict(error=fault_string))
            except HttpAccessTokenRefreshError as e:
                ex_token_error = "Token has been expired or revoked"
                if ex_token_error in str(e):
                    return Response(status=HTTP_400_BAD_REQUEST,
                                    data=dict(error=ex_token_error))
            else:
                mcc_accounts = list(filter(
                    lambda i: i["canManageClients"] and not i["testAccount"],
                    customers,
                ))
                if not mcc_accounts:
                    return Response(
                        status=HTTP_400_BAD_REQUEST,
                        data=dict(error=self.no_mcc_error)
                    )
                with transaction.atomic():
                    user = self.request.user
                    relation = AWConnectionToUserRelation.objects.create(
                        user=user,
                        connection=connection,
                    )
                    user.add_custom_user_group(PermissionGroupNames.SELF_SERVICE)
                    user.add_custom_user_group(PermissionGroupNames.SELF_SERVICE_TRENDS)

                    for ac_data in mcc_accounts:
                        data = dict(
                            id=ac_data["customerId"],
                            name=ac_data["descriptiveName"],
                            currency_code=ac_data["currencyCode"],
                            timezone=ac_data["dateTimeZone"],
                            can_manage_clients=ac_data["canManageClients"],
                            is_test_account=ac_data["testAccount"],
                        )
                        obj, _ = Account.objects.get_or_create(
                            id=data["id"], defaults=data,
                        )
                        AWAccountPermission.objects.get_or_create(
                            aw_connection=connection, account=obj,
                        )
                upload_initial_aw_data.delay(connection.email)

                response = AWAccountConnectionRelationsSerializer(relation).data
                return Response(data=response)

    def get_flow(self, redirect_url):
        aw_settings = load_web_app_settings()
        flow = client.OAuth2WebServerFlow(
            client_id=aw_settings.get("client_id"),
            client_secret=aw_settings.get("client_secret"),
            scope=self.scopes,
            user_agent=aw_settings.get("user_agent"),
            redirect_uri=redirect_url,
        )
        return flow

    @staticmethod
    def delete(request, email, **_):
        try:
            user_connection = AWConnectionToUserRelation.objects.get(
                user=request.user,
                connection__email=email,
            )
            user_connection.delete()
        except AWConnectionToUserRelation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        qs = AWConnectionToUserRelation.objects.filter(
            user=request.user
        ).order_by("connection__email")
        data = AWAccountConnectionRelationsSerializer(qs, many=True).data
        return Response(data)
