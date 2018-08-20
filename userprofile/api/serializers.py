"""
Userprofile api serializers module
"""
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login, PermissionsMixin, \
    Group
from django.core.validators import RegexValidator, EmailValidator, \
    MaxLengthValidator
from django.utils.translation import gettext_lazy as _
from rest_framework.authtoken.models import Token
from rest_framework.serializers import ModelSerializer, CharField, \
    ValidationError, SerializerMethodField, Serializer, EmailField
from rest_framework.validators import UniqueValidator

from administration.notifications import send_new_registration_email, \
    send_welcome_email
from aw_reporting.models import Ad

PHONE_REGEX = RegexValidator(
    regex=r'^\+?1?\d{9,15}$',
    message="Phone number must be entered"
            " in the format: '+999999999'. Up to 15 digits allowed."
)


class UserCreateSerializer(ModelSerializer):
    """
    Serializer for create user
    """
    first_name = CharField(max_length=255, required=True)
    last_name = CharField(max_length=255, required=True)
    company = CharField(max_length=255, required=True)
    phone_number = CharField(
        max_length=15, required=True, validators=[PHONE_REGEX])
    verify_password = CharField(max_length=255, required=True)
    email = EmailField(
        max_length=254,
        validators=[
            UniqueValidator(
                queryset=get_user_model().objects.all(),
                message="Looks like you already have an account"
                        " with this email address. Please try to login"),
            MaxLengthValidator,
            EmailValidator]
    )

    class Meta:
        """
        Meta params
        """
        model = get_user_model()
        fields = (
            "first_name",
            "last_name",
            "company",
            "phone_number",
            "email",
            "password",
            "verify_password"
        )
        read_only_fields = (
            "verify_password",
        )

    def validate(self, data):
        """
        Check password is equal to verify password
        """
        if not data.get("password") == data.pop("verify_password"):
            raise ValidationError("Password and verify password don't match")
        return data

    def save(self, **kwargs):
        """
        Make 'post-save' actions
        """
        user = super(UserCreateSerializer, self).save(**kwargs)
        # set password
        user.set_password(user.password)
        user.save(update_fields=["password"])

        # new default access implementation
        for group_name in settings.DEFAULT_PERMISSIONS_GROUP_NAMES:
            user.add_custom_user_group(group_name)

        # set token
        Token.objects.get_or_create(user=user)
        # update last login
        update_last_login(None, user)
        # send email to admin
        email_data = {
            "host": self.context.get("request").get_host(),
            "email": user.email,
            "company": user.company,
            "phone": user.phone_number,
            "first_name": user.first_name,
            "last_name": user.last_name
        }
        send_new_registration_email(email_data)
        send_welcome_email(user, self.context.get("request"))
        return user


class UserSerializer(ModelSerializer):
    """
    Serializer for update/retrieve user
    """
    first_name = CharField(max_length=255, required=True)
    last_name = CharField(max_length=255, required=True)
    company = CharField(max_length=255, required=True)
    phone_number = CharField(
        max_length=15, required=True, validators=[PHONE_REGEX])
    token = SerializerMethodField()
    has_aw_accounts = SerializerMethodField()
    has_disapproved_ad = SerializerMethodField()
    can_access_media_buying = SerializerMethodField()

    class Meta:
        """
        Meta params
        """
        model = get_user_model()
        fields = (
            "id",
            "first_name",
            "last_name",
            "company",
            "phone_number",
            "email",
            "is_staff",
            "last_login",
            "date_joined",
            "token",
            "access",
            "aw_settings",
            "has_aw_accounts",
            "profile_image_url",
            "can_access_media_buying",
            "has_disapproved_ad",
            "historical_aw_account",
            "google_account_id",
            "logo_url",
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


class UserSetPasswordSerializer(Serializer):
    """
    Serializer for password set endpoint.
    """
    new_password = CharField(required=True)
    email = CharField(required=True)
    token = CharField(required=True)


class UserChangePasswordSerializer(Serializer):
    """
    Serializer for changing user's password.
    """
    new_password = CharField(required=True)
    old_password = CharField(required=True)


class GroupSerializer(Serializer):
    name = CharField(read_only=True)

    class Meta:
        model = Group
        fields = (
            'name',
        )


class ContactFormSerializer(Serializer):
    """
    Serializer for contact form fields
    """
    first_name = CharField(required=True, max_length=255)
    last_name = CharField(required=True, max_length=255)
    email = EmailField(required=True, max_length=255)
    country = CharField(required=True, max_length=255)
    company = CharField(required=True, max_length=255)
    message = CharField(
        required=False,
        max_length=255,
        default="",
        allow_blank=True
    )


class ErrorReportSerializer(Serializer):
    email = EmailField(max_length=255)
    message = CharField(required=True)
