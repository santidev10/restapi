from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login
from django.core.validators import EmailValidator
from django.core.validators import MaxLengthValidator
from rest_framework.authtoken.models import Token
from rest_framework.serializers import CharField
from rest_framework.serializers import EmailField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError
from rest_framework.validators import UniqueValidator

from administration.notifications import send_new_registration_email
from administration.notifications import send_welcome_email
from userprofile.api.serializers.validators import phone_validator


class UserCreateSerializer(ModelSerializer):
    """
    Serializer for create user
    """
    first_name = CharField(max_length=255, required=True)
    last_name = CharField(max_length=255, required=True)
    company = CharField(max_length=255, required=True)
    phone_number = CharField(max_length=15, required=True, validators=[phone_validator])
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
