"""
Administration api serializers module
"""
from django.contrib.auth import get_user_model
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
    class Meta:
        """
        Meta params
        """
        model = get_user_model()
        fields = (
            "plan",
            "can_access_media_buying",
            "pre_baked_segments",
        )

    def save(self, **kwargs):
        """
        Make 'post-save' actions
        """
        user = super(UserUpdateSerializer, self).save(**kwargs)
        if "plan" in kwargs:
            Subscription.objects.filter(user=user).delete()
            plan = Plan.objects.get(name=user.plan.name)
            subscription = Subscription.objects.create(user=user, plan=plan)
            user.update_permissions_from_subscription(subscription)


class UserSerializer(ModelSerializer):
    """
    Retrieve user serializer
    """
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
            "pre_baked_segments",
        )
