"""
Userprofile api serializers module
"""
from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login
from rest_framework.authtoken.models import Token
from rest_framework.serializers import ModelSerializer, CharField, \
    ValidationError, SerializerMethodField, RegexValidator, Serializer, \
    EmailField

from administration.notifications import send_new_registration_email
from userprofile.models import Subscription, Plan
from payments.api.serializers import PlanSerializer as PaymentPlanSerializer
from payments.api.serializers import SubscriptionSerializer as PaymentSubscriptionSerializer

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
        # create default subscription
        plan = Plan.objects.get(name='free')
        subscription = Subscription.objects.create(user=user, plan=plan)
        user.update_permissions_from_subscription(subscription)
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
    plan = SerializerMethodField()

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
            "profile_image_url",
            "can_access_media_buying",
        )
        read_only_fields = (
            "is_staff",
            "last_login",
            "date_joined",
            "token",
            "has_aw_accounts",
            "profile_image_url",
            "can_access_media_buying",
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

    def get_plan(self, obj):
        return obj.plan.name


class UserSetPasswordSerializer(Serializer):
    """
    Serializer for password set endpoint.
    """
    new_password = CharField(required=True)
    email = CharField(required=True)
    token = CharField(required=True)


class PlanSerializer(ModelSerializer):

    payments_plan = SerializerMethodField()

    """
    Permission plan serializer
    """
    class Meta:
        model = Plan
        fields = (
            'name',
            'description',
            'permissions',
            'features',
            'payments_plan',
        )

    def get_payments_plan(self, obj):
        if obj.payments_plan_id is None:
            return {}
        return PaymentPlanSerializer(obj.payments_plan).data


class ContactFormSerializer(Serializer):
    """
    Serializer for contact form fields
    """
    first_name = CharField(required=True, max_length=255)
    last_name = CharField(required=True, max_length=255)
    email = EmailField(required=True, max_length=255)
    country = CharField(required=True, max_length=255)
    message = CharField(
        required=False, max_length=255, allow_null=True, allow_blank=True)
    company = CharField(
        required=False, max_length=255, allow_null=True, allow_blank=True)


class SubscriptionSerializer(Serializer):
    plan = SerializerMethodField()
    payments_subscription = SerializerMethodField()

    class Meta:
        model = Subscription
        fields = (
            "plan",
            "payments_subscription",
        )

    def get_plan(self, obj):
        return PlanSerializer(obj.plan).data

    def get_payments_subscription(self, obj):
        if obj.payments_subscription is None:
            return dict()
        return PaymentSubscriptionSerializer(obj.payments_subscription).data

