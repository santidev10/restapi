"""
Administration api serializers module
"""
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.contrib.auth.models import PermissionsMixin
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import URLField
from rest_framework.serializers import CharField
from rest_framework.serializers import SerializerMethodField
from rest_framework.serializers import ListField
from rest_framework.serializers import BooleanField
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.validators import ValidationError

from administration.models import UserAction
from userprofile.api.serializers.validators.extended_enum import extended_enum
from userprofile.constants import UserStatuses
from userprofile.models import get_default_accesses
from userprofile.permissions import PermissionGroupNames

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
            "phone_number_verified",
            "email",
            "is_staff",
            "last_login",
            "date_joined",
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
    access = ListField(required=False)
    admin = BooleanField(required=False)

    def validate(self, data):
        """
        Check if user is superuser before allowing to change admin status
        :param data: request data
        :return: data
        """
        user = self.context["request"].user
        target = self.instance
        access = data.pop("access", [])
        status = data.get("status", None)
        # Reject if changing own status or target is admin
        if (status and target.id == user.id) or target.is_superuser:
            exception = ValidationError("You do not have permission to perform this action.")
            exception.status_code = HTTP_403_FORBIDDEN
            raise exception
        try:
            # Reject if changing admin access and auth user is not superuser
            admin_access = [item for item in access if item["name"].lower() == "admin"][0]
            if target.is_staff != admin_access["value"] and user.is_superuser is False:
                exception = ValidationError("You do not have permission to perform this action.")
                exception.status_code = HTTP_403_FORBIDDEN
                raise exception
            else:
                data["admin"] = admin_access["value"]
        except (KeyError, IndexError):
            pass
        return data

    class Meta:
        model = get_user_model()
        fields = (
            "status", "access", "admin",
        )

    def save(self, **kwargs):
        old_status = self.instance.status
        user = super(UserUpdateSerializer, self).save(**kwargs)
        request = self.context.get("request")
        status = request.data.get("status", None)
        access = request.data.get("access", [])

        admin = self.validated_data.get("admin", None)
        # If setting admin status, give all access
        if admin == True and admin != user.is_staff:
            user.groups.set(Group.objects.exclude(name=PermissionGroupNames.MANAGED_SERVICE_PERFORMANCE_DETAILS))
            user.is_staff = True
        # If revoking admin status, set default access
        elif admin == False and admin != user.is_staff:
            default_access = get_default_accesses()
            user.groups.clear()
            user.groups.set(Group.objects.filter(name__in=default_access))
            user.is_staff = False
        else:
            user.update_access(access)

        if status:
            if user.status in (UserStatuses.PENDING.value, UserStatuses.REJECTED.value):
                user.is_active = False
            if user.status == UserStatuses.ACTIVE.value:
                user.is_active = True
            if old_status != user.status and user.status == UserStatuses.ACTIVE.value:
                user.email_user_active(request)
        user.save()
        return user
