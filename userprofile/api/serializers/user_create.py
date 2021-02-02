from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login
from django.core.validators import EmailValidator
from django.core.validators import MaxLengthValidator
from rest_framework.serializers import CharField
from rest_framework.serializers import EmailField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError
from rest_framework.validators import UniqueValidator

from administration.notifications import send_new_registration_email
from administration.notifications import send_welcome_email
from userprofile.api.serializers.validators import phone_validator
from userprofile.api.serializers.validators.extended_enum import extended_enum
from userprofile.constants import UserAnnualAdSpend
from userprofile.constants import UserStatuses
from userprofile.constants import UserTypeRegular
from userprofile.models import UserDeviceToken
from userprofile.models import WhiteLabel
from userprofile.models import get_default_accesses
from userprofile.validators import password_validators
from utils.lang import get_request_prefix


class UserCreateSerializer(ModelSerializer):
    """
    Serializer for create user
    """
    first_name = CharField(max_length=255, required=True)
    last_name = CharField(max_length=255, required=True)
    company = CharField(max_length=255, required=True)
    phone_number = CharField(max_length=15, required=True, validators=[phone_validator])
    verify_password = CharField(max_length=255, required=True, validators=[
        password_validators.upper_case_password_validator,
        password_validators.min_length_password_validator,
        password_validators.special_char_password_validator,
        password_validators.numeric_password_validator
    ])
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
    annual_ad_spend = CharField(max_length=255, required=True, allow_blank=False, allow_null=False,
                                validators=[extended_enum(UserAnnualAdSpend)])
    user_type = CharField(max_length=255, required=True, allow_blank=False, allow_null=False,
                          validators=[extended_enum(UserTypeRegular)])
    domain = CharField(max_length=255, required=False)

    class Meta:
        """
        Meta params
        """
        model = get_user_model()
        fields = (
            "annual_ad_spend",
            "company",
            "domain",
            "email",
            "first_name",
            "last_name",
            "password",
            "phone_number",
            "verify_password",
            "user_type",
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

    def get_domain(self):
        request = self.context.get("request")
        domain = WhiteLabel.extract_sub_domain(request.get_host() or "")
        domain_obj = WhiteLabel.get(domain)
        return domain_obj

    def save(self, **kwargs):
        """
        Make 'post-save' actions
        """
        user = super(UserCreateSerializer, self).save(**kwargs)
        # set password
        user.set_password(user.password)
        user.status = UserStatuses.PENDING.value
        user.is_active = False
        user.domain = self.get_domain()
        user.save(update_fields=["password", "status", "is_active", "domain"])

        # update last login
        update_last_login(None, user)
        # set token
        request = self.context.get("request")
        if not request.auth:
            device_token = UserDeviceToken.objects.create(user=user)
            request.auth = device_token
            # send email to admin
        host = request.get_host()
        prefix = get_request_prefix(request)
        email_data = {
            "host": host,
            "email": user.email,
            "company": user.company,
            "phone": user.phone_number,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "annual_ad_spend": user.annual_ad_spend,
            "user_type": user.user_type,
            "user_list_link": "{}{}/admin/users".format(prefix, host),
        }
        send_new_registration_email(email_data)
        send_welcome_email(user, self.context.get("request"))
        return user
