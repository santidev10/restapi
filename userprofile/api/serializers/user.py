from django.contrib.auth import get_user_model
from django.contrib.auth.models import PermissionsMixin
from django.utils.translation import gettext_lazy as _
from rest_framework.authtoken.models import Token
from rest_framework.serializers import CharField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField
from rest_framework.serializers import ValidationError

from aw_reporting.models import Ad
from userprofile.api.serializers.validators import phone_validator


class UserSerializer(ModelSerializer):
    """
    Serializer for update/retrieve user
    """
    can_access_media_buying = SerializerMethodField()
    company = CharField(max_length=255, required=True)
    first_name = CharField(max_length=255, required=True)
    has_aw_accounts = SerializerMethodField()
    has_disapproved_ad = SerializerMethodField()
    last_name = CharField(max_length=255, required=True)
    phone_number = CharField(max_length=15, required=True, validators=[phone_validator])
    token = SerializerMethodField()

    class Meta:
        """
        Meta params
        """
        model = get_user_model()
        fields = (
            "access",
            "aw_settings",
            "can_access_media_buying",
            "company",
            "date_joined",
            "email",
            "first_name",
            "google_account_id",
            "has_aw_accounts",
            "has_disapproved_ad",
            "id",
            "is_staff",
            "last_login",
            "last_name",
            "logo_url",
            "phone_number",
            "profile_image_url",
            "token",
        )
        read_only_fields = (
            "is_staff",
            "last_login",
            "date_joined",
            "token",
            "has_aw_accounts",
            "profile_image_url",
            "can_access_media_buying",
            "google_account_id",
            "logo_url",
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

    def get_token(self, obj):
        """
        Obtain user auth token
        """
        try:
            return obj.auth_token.key
        except Token.DoesNotExist:
            return

    def get_can_access_media_buying(self, obj: PermissionsMixin):
        return obj.has_perm("userprofile.view_media_buying")

    def validate_historical_aw_account(self, connection):
        if connection is None:
            return connection
        if not self.instance.aw_connections.filter(id=connection.id).exists():
            raise ValidationError(
                _("Historical account should be listed in user connections"))
        return connection
