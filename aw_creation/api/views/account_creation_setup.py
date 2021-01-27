from django.db.models import Q
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.generics import RetrieveUpdateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_creation.api.serializers import *
from aw_creation.email_messages import send_tracking_tags_request
from aw_creation.models import AccountCreation
from aw_reporting.adwords_api import create_customer_account
from aw_reporting.adwords_api import handle_aw_api_errors
from aw_reporting.adwords_api import update_customer_account
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.views import forbidden_for_demo
from aw_reporting.models import AWConnection
from aw_reporting.models import Account
from userprofile.constants import StaticPermissions


def is_demo(*args, **kwargs):
    str_pk = kwargs.get("pk")
    return str_pk.isnumeric() and int(str_pk) == DEMO_ACCOUNT_ID


class AccountCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = AccountCreationSetupSerializer
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MEDIA_BUYING),)

    @swagger_auto_schema(
        operation_description="Get account creation",
        manual_parameters=[
            openapi.Parameter(
                name="id",
                required=True,
                in_=openapi.IN_PATH,
                description="Account creation id",
                type=openapi.TYPE_STRING,
            ),
        ],
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    @swagger_auto_schema(
        operation_description="""Update account creation.
        To push account to AW set `is_approved=True`""",
        manual_parameters=[
            openapi.Parameter(
                name="id",
                required=True,
                in_=openapi.IN_PATH,
                description="Account creation id",
                type=openapi.TYPE_STRING,
            ),
        ],
    )
    def put(self, request, *args, **kwargs):
        return super().put(request, *args, **kwargs)

    def get_queryset(self):
        queryset = AccountCreation.objects.filter(Q(owner=self.request.user) | Q(account_id=DEMO_ACCOUNT_ID)) \
            .filter(is_managed=True)
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

    # pylint: disable=too-many-nested-blocks,too-many-return-statements
    @forbidden_for_demo(is_demo)
    def update(self, request, *args, **kwargs):
        partial = kwargs.pop("partial", False)
        instance = self.get_object()
        data = request.data
        # approve rules
        if "is_approved" in data:
            if data["is_approved"]:
                if not instance.account:  # create account
                    # check dates
                    today = instance.get_today_date()
                    for campaign in instance.campaign_creations.all():
                        if campaign.start and campaign.start < today or campaign.end and campaign.end < today:
                            return Response(
                                status=HTTP_400_BAD_REQUEST,
                                data=dict(error="The dates cannot be in the past: {}".format(campaign.name)))
                    self._validate_consistency(instance)
                    mcc_accounts = Account.user_mcc_objects(request.user)
                    if not mcc_accounts.exists():
                        return Response(
                            status=HTTP_400_BAD_REQUEST, data=dict(error="You have no connected MCC account"))
                    try:
                        mcc_account = mcc_accounts.get(id=request.data.get("mcc_account_id"))
                    except Account.DoesNotExist:
                        return Response(
                            status=HTTP_400_BAD_REQUEST, data=dict(error="Wrong MCC account was selected"))
                    connection = AWConnection.objects.filter(
                        mcc_permissions__account=mcc_account,
                        user_relations__user=request.user,
                        revoked_access=False,
                    ).first()
                    _, error = handle_aw_api_errors(self.account_creation, instance, mcc_account, connection)
                    if error:
                        return Response(status=HTTP_400_BAD_REQUEST, data=dict(error=error))
                send_tracking_tags_request(request.user, instance)
            elif instance.account:
                return Response(status=HTTP_400_BAD_REQUEST, data=dict(
                    error="You cannot disapprove a running account"))
        if "name" in data and data["name"] != instance.name and instance.account:
            connection = AWConnection.objects.filter(
                mcc_permissions__account__in=instance.account.managers.all(),
                user_relations__user=request.user,
                revoked_access=False,
            ).values("mcc_permissions__account_id", "refresh_token").first()
            if connection:
                _, error = handle_aw_api_errors(
                    update_customer_account,
                    connection["mcc_permissions__account_id"],
                    connection["refresh_token"],
                    instance.account.id,
                    data["name"],
                )
                if error:
                    return Response(status=HTTP_400_BAD_REQUEST, data=dict(error=error))
        serializer = AccountCreationUpdateSerializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)
        return self.retrieve(self, request, *args, **kwargs)

    # pylint: enable=too-many-nested-blocks,too-many-return-statements

    @forbidden_for_demo(is_demo)
    def delete(self, request, *args, **kwargs):
        instance = self.get_object()
        if instance.account is not None:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data=dict(error="You cannot delete approved setups"))
        AccountCreation.objects.filter(pk=instance.id).update(is_deleted=True)
        return Response(status=HTTP_204_NO_CONTENT)

    def _validate_consistency(self, account_creation):
        for campaign_creation in account_creation.campaign_creations.all():
            self._validate_campaign_creation(campaign_creation)

    def _validate_campaign_creation(self, campaign_creation):
        for ad_group_creation in campaign_creation.ad_group_creations.all():
            self._validate_ad_group_creation(ad_group_creation)

    def _validate_ad_group_creation(self, ad_group_creation):
        self._validate_discovery_ad_group(ad_group_creation)

    def _validate_discovery_ad_group(self, ad_group_creation):
        if ad_group_creation.video_ad_format != AdGroupCreation.DISCOVERY_TYPE:
            return
        for ad_creation in ad_group_creation.ad_creations.all():
            self._validate_discovery_ad(ad_creation)

    def _validate_discovery_ad(self, ad_creation):
        for field in ("headline",):
            value = getattr(ad_creation, field)
            if value is None:
                raise ValidationError("{} can't be null".format(field))
            if value == "":
                raise ValidationError("{} can't be empty".format(field))
