from rest_framework.generics import RetrieveUpdateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_204_NO_CONTENT

from aw_creation.api.serializers import *
from aw_creation.email_messages import send_tracking_tags_request
from aw_creation.models import AccountCreation
from aw_reporting.adwords_api import create_customer_account, \
    update_customer_account, handle_aw_api_errors
from aw_reporting.demo.decorators import demo_view_decorator
from aw_reporting.models import Account, AWConnection
from utils.permissions import MediaBuyingAddOnPermission, user_has_permission, \
    or_permission_classes


@demo_view_decorator
class AccountCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = AccountCreationSetupSerializer
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.settings_my_aw_accounts"),
            MediaBuyingAddOnPermission),
    )

    def get_queryset(self):
        queryset = AccountCreation.objects.filter(owner=self.request.user,is_managed=True)
        return queryset

    def account_creation(self, account_creation, mcc_account, connection):
        aw_id = create_customer_account(
            mcc_account.id, connection.refresh_token,
            account_creation.name, mcc_account.currency_code,
            mcc_account.timezone,
        )
        # save to db
        customer = Account.objects.create(
            id=aw_id,
            name=account_creation.name,
            currency_code=mcc_account.currency_code,
            timezone=mcc_account.timezone,
            skip_creating_account_creation=True,
        )
        customer.managers.add(mcc_account)
        account_creation.account = customer
        account_creation.save()

        return customer

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        data = request.data
        # approve rules
        if "is_approved" in data:
            if data["is_approved"]:
                if not instance.account:  # create account
                    # check dates
                    today = instance.get_today_date()
                    for c in instance.campaign_creations.all():
                        if c.start and c.start < today or c.end and c.end < today:
                            return Response(status=HTTP_400_BAD_REQUEST,
                                            data=dict(
                                                error="The dates cannot be in the past: {}".format(
                                                    c.name)))
                    try:
                        mcc_account = Account.user_mcc_objects(request.user).get(id=request.data.get("mcc_account_id"))
                    except Account.DoesNotExist:
                        return Response(
                            status=HTTP_400_BAD_REQUEST, data=dict(error="Wrong account were selected"))
                    if mcc_account:
                        connection = AWConnection.objects.filter(
                            mcc_permissions__account=mcc_account,
                            user_relations__user=request.user,
                            revoked_access=False,
                        ).first()
                        _, error = handle_aw_api_errors(self.account_creation,
                                                        instance, mcc_account,
                                                        connection)
                        if error:
                            return Response(status=HTTP_400_BAD_REQUEST,
                                            data=dict(error=error))
                    else:
                        return Response(status=HTTP_400_BAD_REQUEST,
                                        data=dict(
                                            error="You have no connected MCC account"))
                send_tracking_tags_request(request.user, instance)

            elif instance.account:
                return Response(status=HTTP_400_BAD_REQUEST, data=dict(
                    error="You cannot disapprove a running account"))

        if "name" in data and data["name"] != instance.name and instance.account:
            connections = AWConnection.objects.filter(
                mcc_permissions__account=instance.account.managers.all(),
                user_relations__user=request.user,
                revoked_access=False,
            ).values("mcc_permissions__account_id", "refresh_token")
            if connections:
                connection = connections[0]
                _, error = handle_aw_api_errors(
                    update_customer_account,
                    connection['mcc_permissions__account_id'],
                    connection['refresh_token'],
                    instance.account.id,
                    data['name'],
                )
                if error:
                    return Response(status=HTTP_400_BAD_REQUEST,
                                    data=dict(error=error))

        serializer = AccountCreationUpdateSerializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)
        return self.retrieve(self, request, *args, **kwargs)

    def delete(self, request, *args, **kwargs):
        instance = self.get_object()
        if instance.account is not None:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="You cannot delete approved setups"))
        AccountCreation.objects.filter(pk=instance.id).update(is_deleted=True)
        return Response(status=HTTP_204_NO_CONTENT)
