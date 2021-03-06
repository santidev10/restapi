import boto3
from botocore.exceptions import ClientError
from django.conf import settings
from django.contrib.auth import get_user_model
from rest_framework.exceptions import ValidationError
from rest_framework.serializers import CharField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField
from rest_framework import serializers

from aw_reporting.models import Ad
from userprofile.api.serializers.validators import phone_validator
from userprofile.constants import StaticPermissions
from userprofile.constants import UserSettingsKey
from userprofile.models import WhiteLabel
from userprofile.api.serializers.permission_mixin import PermissionSerializerMixin


class UserSerializer(ModelSerializer, PermissionSerializerMixin):
    """
    Serializer for update/retrieve user
    """
    aw_settings = SerializerMethodField()
    can_access_media_buying = SerializerMethodField()
    company = CharField(max_length=255, required=True)
    first_name = CharField(max_length=255, required=True)
    has_aw_accounts = SerializerMethodField()
    has_disapproved_ad = SerializerMethodField()
    last_name = CharField(max_length=255, required=True)
    phone_number = CharField(max_length=15, required=True, validators=[phone_validator])
    user_type = CharField(max_length=255)
    domain = CharField(max_length=255)
    role_id = serializers.IntegerField(source="user_role.role_id")
    perms = serializers.SerializerMethodField()

    class Meta:
        """
        Meta params
        """
        model = get_user_model()
        fields = (
            "aw_settings",
            "can_access_media_buying",
            "company",
            "date_joined",
            "domain",
            "email",
            "first_name",
            "google_account_id",
            "has_aw_accounts",
            "has_disapproved_ad",
            "id",
            "is_staff",
            "is_active",
            "last_login",
            "last_name",
            "logo_url",
            "perms",
            "phone_number",
            "phone_number_verified",
            "profile_image_url",
            "has_accepted_GDPR",
            "role_id",
            "user_type",
        )
        read_only_fields = (
            "is_staff",
            "last_login",
            "date_joined",
            "has_aw_accounts",
            "profile_image_url",
            "can_access_media_buying",
            "google_account_id",
            "logo_url",
            "is_active",
            "aw_settings",
            "user_type",
        )


    @staticmethod
    def get_has_aw_accounts(obj):
        return obj.aw_connections.count() > 0

    @staticmethod
    def get_has_disapproved_ad(obj):
        return Ad.objects \
            .filter(is_disapproved=True,
                    ad_group__campaign__account__mcc_permissions__aw_connection__user_relations__user=obj) \
            .exists()

    def get_can_access_media_buying(self, obj: get_user_model()):
        return obj.has_permission(StaticPermissions.MEDIA_BUYING)

    def validate_sub_domain(self, sub_domain):
        sub_domain_obj = WhiteLabel.get(sub_domain)
        return sub_domain_obj

    def validate_phone_number(self, phone_number):
        client = boto3.client("cognito-idp")
        user = self.context["request"].user
        user_attributes = [{"Name": "phone_number", "Value": phone_number}]
        try:
            client.admin_update_user_attributes(
                UserPoolId=settings.COGNITO_USER_POOL_ID,
                Username=user.email,
                UserAttributes=user_attributes
            )
        except ClientError as err:
            error = err.response["Error"]["Message"]
            raise ValidationError(error)
        return phone_number

    def get_aw_settings(self, obj):
        """ Remove deprecated fields """
        aw_settings = {
            key: value for key, value in obj.aw_settings.items()
            if key in UserSettingsKey.ACTIVE_AW_SETTINGS_KEYS
        }
        return aw_settings
