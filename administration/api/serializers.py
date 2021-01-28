"""
Administration api serializers module
"""
from django.contrib.auth import get_user_model
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import ListField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField
from rest_framework.serializers import URLField
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.validators import ValidationError

from administration.models import UserAction
from userprofile.constants import StaticPermissions
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
        return None

    def get_first_name(self, obj):
        """
        User first name
        """
        if obj.user is not None:
            return obj.user.first_name
        return None

    def get_last_name(self, obj):
        """
        User last name
        """
        if obj.user is not None:
            return obj.user.last_name
        return None


class UserSerializer(ModelSerializer):
    """
    Retrieve user serializer
    """
    can_access_media_buying = SerializerMethodField()
    domain = CharField(max_length=255)

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
            "domain",
            "phone_number",
            "phone_number_verified",
            "email",
            "is_staff",
            "last_login",
            "date_joined",
            "perms",
            "google_account_id",
            "can_access_media_buying",
            "annual_ad_spend",
            "user_type",
            "status"
        )

    def get_can_access_media_buying(self, obj):
        return obj.has_permission(StaticPermissions.MEDIA_BUYING)


class UserUpdateSerializer(ModelSerializer):
    status = CharField(max_length=255, required=True, allow_blank=False, allow_null=False,
                       validators=[extended_enum(UserStatuses)])
    admin = BooleanField(required=False)

    def validate(self, attrs):
        """
        Check if user is superuser before allowing to change admin status
        :param data: request data
        :return: data
        """
        data = attrs
        user = self.context["request"].user
        target = self.instance
        status = data.get("status", None)
        # Reject if changing own status or target is admin
        if (status and target.id == user.id) or target.has_permission(StaticPermissions.ADMIN):
            exception = ValidationError("You do not have permission to perform this action.")
            exception.status_code = HTTP_403_FORBIDDEN
            raise exception
        return data

    class Meta:
        model = get_user_model()
        fields = (
            "status", "admin",
        )

    def save(self, **kwargs):
        old_status = self.instance.status
        user = super(UserUpdateSerializer, self).save(**kwargs)
        request = self.context.get("request")
        status = request.data.get("status", None)

        if status:
            if user.status in (UserStatuses.PENDING.value, UserStatuses.REJECTED.value):
                user.is_active = False
            if user.status == UserStatuses.ACTIVE.value:
                user.is_active = True
            if old_status != user.status and user.status == UserStatuses.ACTIVE.value:
                user.email_user_active(request)
        user.save()
        return user
