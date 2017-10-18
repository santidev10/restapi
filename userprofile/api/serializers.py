"""
Userprofile api serializers module
"""
from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login
from rest_framework.authtoken.models import Token
from rest_framework.serializers import ModelSerializer, CharField, \
    ValidationError, SerializerMethodField, RegexValidator, Serializer

from administration.notifications import send_new_registration_email
from userprofile.models import Plan

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
        user.save()
        # set token
        Token.objects.get_or_create(user=user)
        # update last login
        update_last_login(None, user)
        # send email to admin
        email_data = {
            "host": self.context.get("request").get_host(),
            "email": user.email,
            "company": user.company,
            "phone": user.phone_number
        }
        send_new_registration_email(email_data)
        # done
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
            "has_aw_accounts",
            "plan",
        )
        read_only_fields = (
            "is_staff",
            "last_login",
            "date_joined",
            "token",
            "has_aw_accounts",
        )

    @staticmethod
    def get_has_aw_accounts(obj):
        return obj.aw_connections.count() > 0

    def get_token(self, obj):
        """
        Obtain user auth token
        """
        try:
            return obj.auth_token.key
        except Token.DoesNotExist:
            return


class UserSetPasswordSerializer(Serializer):
    """
    Serializer for password set endpoint.
    """
    new_password = CharField(required=True)
    email = CharField(required=True)
    token = CharField(required=True)


class PlanSerializer(ModelSerializer):
    """
    Permission plan serializer
    """
    class Meta:
        model = Plan
        fields = {
            'name',
            'permissions',
        }

