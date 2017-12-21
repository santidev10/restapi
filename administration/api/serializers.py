"""
Administration api serializers module
"""
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import PermissionsMixin
from rest_framework.serializers import ModelSerializer, URLField, CharField, \
    SerializerMethodField

from administration.models import UserAction
from userprofile.models import Subscription, Plan


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


class UserUpdateSerializer(ModelSerializer):
    """
    Update user serializer
    """
    can_access_media_buying = SerializerMethodField()

    class Meta:
        """
        Meta params
        """
        model = get_user_model()
        fields = (
            "plan",
            "can_access_media_buying",
        )

    def save(self, **kwargs):
        """
        Make 'post-save' actions
        """
        user = super(UserUpdateSerializer, self).save(**kwargs)
        Subscription.objects.filter(user=user).delete()
        if user.plan_id is None:
            user.plan_id = settings.DEFAULT_ACCESS_PLAN_NAME
        plan = Plan.objects.get(name=user.plan_id)
        subscription = Subscription.objects.create(user=user, plan=plan)
        user.update_permissions_from_subscription(subscription)
        user.save()
        return user

    def get_can_access_media_buying(self, obj):
        return obj.has_perm("view_media_buying")


class UserSerializer(ModelSerializer):
    """
    Retrieve user serializer
    """
    is_user_paid_for_subscription = SerializerMethodField()
    current_period_end = SerializerMethodField()
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
            "plan",
            "can_access_media_buying",
            "is_user_paid_for_subscription",
            "current_period_end",
        )

    def get_is_user_paid_for_subscription(self, obj):
        try:
            current_subscription = Subscription.objects.get(user=obj)
            return True if current_subscription.payments_subscription else False
        except Subscription.DoesNotExist:
            return False

    def get_current_period_end(self, obj):
        try:
            current_subscription = Subscription.objects.get(user=obj)
            if current_subscription.payments_subscription:
                return current_subscription.payments_subscription.current_period_end
            return False
        except Subscription.DoesNotExist:
            return False

    def get_can_access_media_buying(self, obj: PermissionsMixin):
        return obj.has_perm("userprofile.view_media_buying")
