"""
Userprofile api serializers module
"""
from django.contrib.auth import get_user_model
from rest_framework.authtoken.models import Token
from rest_framework.serializers import ModelSerializer, CharField, \
    ValidationError, SerializerMethodField, RegexValidator


class UserCreateSerializer(ModelSerializer):
    """
    Serializer for create user
    """
    verify_password = CharField(max_length=255, required=True)
    company = CharField(max_length=255, required=True)
    phone_regex = RegexValidator(
        regex=r'^\+?1?\d{9,15}$',
        message="Phone number must be entered"
                " in the format: '+999999999'. Up to 15 digits allowed."
    )
    phone_number = CharField(
        max_length=15, required=True, validators=[phone_regex])

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
        Set user password
        """
        user = super(UserCreateSerializer, self).save(**kwargs)
        user.set_password(user.password)
        user.save()
        Token.objects.get_or_create(user=user)
        return user


class UserSerializer(ModelSerializer):
    """
    Serializer for update/retrieve user
    """
    token = SerializerMethodField()

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
            "token"
        )
        read_only_fields = (
            "is_staff",
            "token"
        )

    def get_token(self, obj):
        """
        Obtain user auth token
        """
        try:
            return obj.auth_token.key
        except Token.DoesNotExist:
            return
