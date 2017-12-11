"""
Userprofile models module
"""
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, \
    UserManager, Permission
from django.contrib.contenttypes.models import ContentType
from django.contrib.postgres.fields import JSONField
from django.conf import settings
from django.core import validators
from django.core.mail import send_mail
from django.db import models
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _

from utils.models import Timestampable


class UserProfile(AbstractBaseUser, PermissionsMixin):
    """
    An abstract base class implementing a fully featured User model with
    admin-compliant permissions.

    Username, password and email are required. Other fields are optional.
    """
    username = models.CharField(
        _('username'), max_length=30, blank=True, null=True,
        help_text=_('Required. 30 characters or fewer. Letters, digits and '
                    '@/./+/-/_ only.'),
        validators=[
            validators.RegexValidator(
                r'^[\w.@+-]+$',
                _('Enter a valid username. '
                    'This value may contain only letters, numbers '
                    'and @/./+/-/_ characters.'), 'invalid'),
        ],
        error_messages={
            'unique': _("A user with that username already exists."),
        })
    first_name = models.CharField(_('first name'), max_length=30, blank=True)
    last_name = models.CharField(_('last name'), max_length=30, blank=True)
    is_staff = models.BooleanField(
        _('staff status'), default=False,
        help_text=_('Designates whether the user can log into this admin '
                    'site.'))
    is_active = models.BooleanField(
        _('active'), default=True,
        help_text=_('Designates whether this user should be treated as '
                    'active. Unselect this instead of deleting accounts.'))
    date_joined = models.DateTimeField(_('date joined'), default=timezone.now)

    # extra fields and updated fields
    email = models.EmailField(_('email address'), unique=True)
    company = models.CharField(max_length=255, null=True, blank=True)
    phone_number = models.CharField(max_length=15, null=True, blank=True)
    profile_image_url = models.URLField(null=True, blank=True)

    plan = models.ForeignKey('userprofile.Plan', null=True, on_delete=models.SET_NULL)
    can_access_media_buying = models.BooleanField(default=False)

    objects = UserManager()

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['username']

    class Meta:
        """
        Meta params
        """
        verbose_name = _('user')
        verbose_name_plural = _('users')

    def __str__(self):
        """
        User instance string representation
        :return: str
        """
        return self.get_full_name() or self.email.split('@')[0]

    def get_full_name(self):
        """
        Returns the first_name plus the last_name, with a space in between.
        """
        full_name = '{} {}'.format(self.first_name, self.last_name)
        return full_name.strip()

    def get_short_name(self):
        """
        Returns the short name for the user
        """
        return self.first_name

    def email_user(self, subject, message, from_email=None, **kwargs):
        """
        Sends an email to this User.
        """
        send_mail(subject, message, from_email, [self.email], **kwargs)

    @property
    def token(self):
        """
        User auth token
        """
        return self.auth_token.key

    def set_permissions_from_plan(self, plan_name):
        """
        Convert plan to django permissions
        """
        try:
            plan = Plan.objects.get(name=plan_name)
        except Plan.DoesNotExist:
            plan, created = Plan.objects.get_or_create(
                name=settings.DEFAULT_ACCESS_PLAN,
                defaults=settings.ACCESS_PLANS[settings.DEFAULT_ACCESS_PLAN])

        self.set_permissions_from_node(plan.permissions)

    def set_permissions_from_node(self, node, path=''):
        self.content_type = ContentType.objects.get_for_model(Plan)
        for key, value in node.items():
            if len(path) > 0:
                new_path = path + '_' + key
            else:
                new_path = key

            if type(value) == dict:
                self.set_permissions_from_node(value, new_path)
            else:
                permission, created = Permission.objects.get_or_create(
                    codename=new_path, defaults=dict(content_type=self.content_type))
                if value:
                    self.user_permissions.add(permission)
                else:
                    self.user_permissions.remove(permission)

    def update_permissions_from_subscription(self, subscription):
        self.plan = subscription.plan
        self.set_permissions_from_plan(self.plan.name)
        self.save()


class Plan(models.Model):
    """
    Default plan
    """

    name = models.CharField(max_length=255, primary_key=True)
    description = models.TextField(blank=True)
    permissions = JSONField(default=dict())
    features = JSONField(default=list())
    payments_plan = models.ForeignKey('payments.Plan', null=True, on_delete=models.SET_NULL)
    hidden = models.BooleanField(default=False)

    @staticmethod
    def update_defaults():
        plan_preset = settings.ACCESS_PLANS
        for key, value in plan_preset.items():
            plan, created = Plan.objects.get_or_create(name=key, defaults=value)
            # update permissions and features
            if not created:
                plan.permissions = value['permissions']
                plan.save()

        # set admin plans
        plan = Plan.objects.get(name='enterprise')
        users = UserProfile.objects.filter(is_staff=True)
        for user in users:
            user.plan = plan
            user.set_permissions_from_plan(plan.name)
            user.save()

        # set default plan for non-admin users
        plan = Plan.objects.get(name=settings.DEFAULT_ACCESS_PLAN_NAME)
        users = UserProfile.objects.filter(plan__isnull=True)
        for user in users:
            user.plan = plan
            user.set_permissions_from_plan(plan.name)
            user.save()

        # tie with the payments
        from payments.models import Plan as PaymentPlan
        plan = Plan.objects.get(name='standard')
        plan.payments_plan = PaymentPlan.objects.get(stripe_id="Standard")
        plan.save()
        plan = Plan.objects.get(name='professional')
        plan.payments_plan = PaymentPlan.objects.get(stripe_id="Professional")
        plan.save()

        for key, value in plan_preset.items():
            plan, created = Plan.objects.get_or_create(name=key, defaults=value)
            if created:
                continue
            plan.permissions = value['permissions']
            plan.save()

            users = UserProfile.objects.filter(plan=plan)
            for user in users:
                user.set_permissions_from_plan(key)
                user.save()


class Subscription(models.Model):
    user = models.ForeignKey(UserProfile, on_delete=models.CASCADE)
    plan = models.ForeignKey(Plan, on_delete=models.CASCADE)
    payments_subscription = models.ForeignKey(
        'payments.Subscription', default=None, null=True, on_delete=models.CASCADE)


class UserChannel(Timestampable):
    channel_id = models.CharField(max_length=30)
    user = models.ForeignKey(UserProfile, related_name="channels")

    class Meta:
        unique_together = ("channel_id", "user")
