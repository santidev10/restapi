"""
Userprofile models module
"""
from django.conf import settings
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, \
    UserManager, Permission
from django.contrib.contenttypes.models import ContentType
from django.contrib.postgres.fields import JSONField
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

    plan = models.ForeignKey('userprofile.Plan', null=True,
                             on_delete=models.SET_NULL)
    permissions = JSONField(default={})
    access = JSONField(default=settings.DEFAULT_USER_ACCESS)

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

    def update_permissions(self, source):
        self.update_permissions_tree(source, self.permissions)
        self.create_custom_permissions(self.permissions)
        self.save()

    def update_permissions_tree(self, source, destination):
        for key, value in source.items():
            if type(value) == dict:
                if destination.get(key) is not None:
                    self.update_permissions_tree(value, destination[key])
                    continue
            destination[key] = value

    def create_custom_permissions(self, node, path=''):
        for key, value in node.items():
            if len(path) > 0:
                new_path = path + '_' + key
            else:
                new_path = key

            if type(value) == dict:
                self.create_custom_permissions(value, new_path)
            else:
                if value:
                    self.add_custom_user_permission(new_path)
                else:
                    self.remove_custom_user_permission(new_path)

    def update_permissions_from_plan(self, plan_name):
        """
        Convert plan to django permissions
        """
        try:
            plan = Plan.objects.get(name=plan_name)
        except Plan.DoesNotExist:
            plan, created = Plan.objects.get_or_create(
                name=settings.DEFAULT_ACCESS_PLAN_NAME,
                defaults=settings.ACCESS_PLANS[
                    settings.DEFAULT_ACCESS_PLAN_NAME])

        self.update_permissions(plan.permissions)

    def update_permissions_from_subscription(self, subscription):
        self.plan = subscription.plan
        self.update_permissions_from_plan(self.plan.name)

    def add_custom_user_permission(self, perm: str):
        permission = get_custom_permission(perm)
        self.user_permissions.add(permission)

    def remove_custom_user_permission(self, perm: str):
        permission = get_custom_permission(perm)
        self.user_permissions.remove(permission)

    def update_access(self, access):
        for key, value in access.items():
            self.apply_access_item(key, value)

    def apply_access_item(self, name, action):
        access = self.access
        logic = settings.USER_ACCESS_LOGIC.get(name)
        if logic is None:
            return
        permissions = dict()
        access[name] = action
        self.access = access
        self.apply_accesss_logic(logic, permissions, action)
        self.update_permissions(permissions)

    def apply_accesss_logic(self, logic, destination, action):
        for key, value in logic.items():
            if type(value) == dict:
                if destination.get(key) is None:
                    destination[key] = {}
                self.apply_accesss_logic(value, destination[key], action)
                continue
            destination[key] = action


def get_custom_permission(codename: str):
    content_type = ContentType.objects.get_for_model(Plan)
    permission, _ = Permission.objects.get_or_create(
        content_type=content_type,
        codename=codename)
    return permission


class Plan(models.Model):
    """
    Default plan
    """

    name = models.CharField(max_length=255, primary_key=True)
    description = models.TextField(blank=True)
    permissions = JSONField(default=dict())
    features = JSONField(default=list())
    payments_plan = models.ForeignKey('payments.Plan', null=True,
                                      on_delete=models.SET_NULL)
    hidden = models.BooleanField(default=False)

    @staticmethod
    def update_defaults():
        plan_preset = settings.ACCESS_PLANS
        for key, value in plan_preset.items():
            plan, created = Plan.objects.get_or_create(name=key, defaults=value)
            # update permissions and features
            if not created:
                plan.permissions = value['permissions']
                plan.hidden = value['hidden']
                plan.save()


class Subscription(models.Model):
    user = models.ForeignKey(UserProfile, on_delete=models.CASCADE)
    plan = models.ForeignKey(Plan, on_delete=models.CASCADE)
    payments_subscription = models.ForeignKey(
        'payments.Subscription', default=None, null=True,
        on_delete=models.CASCADE)


class UserChannel(Timestampable):
    channel_id = models.CharField(max_length=30)
    user = models.ForeignKey(UserProfile, related_name="channels")

    class Meta:
        unique_together = ("channel_id", "user")
