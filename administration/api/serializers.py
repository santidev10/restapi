"""
Administration api serializers module
"""
from django.contrib.auth import get_user_model
from django.contrib.auth.models import PermissionsMixin
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import URLField
from rest_framework.serializers import CharField
from rest_framework.serializers import SerializerMethodField
from rest_framework.serializers import BooleanField
from rest_framework.validators import ValidationError

from administration.models import UserAction
from userprofile.api.serializers.validators.extended_enum import extended_enum
from userprofile.constants import UserStatuses


class UserActionCreateSerializer(ModelSerializer):
    """
    Serializer for create user action instance
    """
    url = URLField(max_length=200, required=True)
    slug = CharField(max_length=20, required=True)

    class Meta:
        """
        Meta params
        """
        model = UserAction
        fields = (
            "slug",
            "user",
            "url"
        )


class UserActionRetrieveSerializer(ModelSerializer):
    """
    Serializer for user action model instances retrieve
    """
    email = SerializerMethodField()
    first_name = SerializerMethodField()
    last_name = SerializerMethodField()

    class Meta:
        """
        Meta params
        """
        model = UserAction
        fields = (
            "id",
            "slug",
            "first_name",
            "last_name",
            "email",
            "url",
            "created_at",
        )

    def get_email(self, obj):
        """
        User email
        """
        if obj.user is not None:
            return obj.user.email

    def get_first_name(self, obj):
        """
        User first name
        """
        if obj.user is not None:
            return obj.user.first_name

    def get_last_name(self, obj):
        """
        User last name
        """
        if obj.user is not None:
            return obj.user.last_name


class UserSerializer(ModelSerializer):
    """
    Retrieve user serializer
    """
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
            "google_account_id",
            "can_access_media_buying",
            "annual_ad_spend",
            "user_type",
            "status"
        )

    def get_can_access_media_buying(self, obj: PermissionsMixin):
        return obj.has_perm("userprofile.view_media_buying")


class UserUpdateSerializer(ModelSerializer):
    status = CharField(max_length=255, required=True, allow_blank=False, allow_null=False,
                       validators=[extended_enum(UserStatuses)])
    staff_status = BooleanField(required=False)

    def validate(self, data):
        """
        Check if user is superuser before allowing to change staff status
        :param data: request data
        :return: data
        """
        user = self.context["request"].user
        if data.get("staff_status") and user.is_superuser is False:
            raise ValidationError("You do not have permission to change staff status.")
        return data

    class Meta:
        model = get_user_model()
        fields = (
            "status", "staff_status",
        )

    def save(self, **kwargs):
        old_status = self.instance.status
        user = super(UserUpdateSerializer, self).save(**kwargs)
        request = self.context.get("request")
        staff_status = request.data.get("staff_status", None)
        access = request.data.get("access", None)
        status = request.data.get("status", None)
        if access:
            user.update_access(access)
        if status:
            if user.status in (UserStatuses.PENDING.value, UserStatuses.REJECTED.value):
                user.is_active = False
            if user.status == UserStatuses.ACTIVE.value:
                user.is_active = True
            if old_status != user.status and user.status == UserStatuses.ACTIVE.value:
                user.email_user_active(request)
        if staff_status:
            user.is_staff = staff_status
        user.save()
        return user
