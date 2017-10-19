"""
Userprofile models module
"""
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, \
    UserManager, Permission
from django.contrib.contenttypes.models import ContentType
from django.core import validators
from django.core.mail import send_mail
from django.db import models
from django.contrib.postgres.fields import JSONField
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

    plan = models.ForeignKey('userprofile.Plan', null=True, on_delete=models.SET_NULL)

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

    def set_permissions_from_plan(self, plan_name):
        """
        Convert plan to django permissions
        """
        try:
            plan = Plan.objects.get(name=plan_name)
        except Plan.DoesNotExist:
            plan, created = Plan.objects.get_or_create(
                name='free', defaults=dict(permissions=Plan.plan_preset['free']))

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


class Plan(models.Model):
    """
    Default plan
    """
    plan_preset = {
        'free': {
            'channel' : {'list': False, 'filter': False, 'audience': False, 'details': False,},
            'video':    {'list': False, 'filter': False, 'audience': False, 'details': False,},
            'keyword':  {'list': False, 'details': False,},
            'segment': {
                'channel': {'all': False, 'private': True},
                'video': {'all': False, 'private': True},
                'keyword': {'all': False, 'private': True},
            },
            'view': {
                'create_and_manage_campaigns': False,
                'performance': False,
                'trends': False,
                'benchmarks': False,
                'highlights': False,
            },
            'settings': {
                'my_yt_channels': True,
                'my_aw_accounts': False,
                'billing': True,
            },
        },
        'full': {
            'channel': {'list': True, 'filter': True, 'audience': True, 'details': True, },
            'video': {'list': True, 'filter': True, 'audience': True, 'details': True, },
            'keyword': {'list': True, 'details': True, },
            'segment': {
                'channel': {'all': True, 'private': True},
                'video': {'all': True, 'private': True},
                'keyword': {'all': True, 'private': True},
            },
            'view': {
                'create_and_manage_campaigns': True,
                'performance': True,
                'trends': True,
                'benchmarks': True,
                'highlights': True,
            },
            'settings': {
                'my_yt_channels': True,
                'my_aw_accounts': True,
                'billing': True,
            },
        },
        'media_buyer': {
            'channel': {'list': True, 'filter': True, 'audience': False, 'details': True, },
            'video': {'list': True, 'filter': True, 'audience': False, 'details': True, },
            'keyword': {'list': True, 'details': True, },
            'segment': {
                'channel': {'all': False, 'private': True},
                'video': {'all': False, 'private': True},
                'keyword': {'all': False, 'private': True},
            },
            'view': {
                'create_and_manage_campaigns': False,
                'performance': False,
                'trends': False,
                'benchmarks': False,
                'highlights': True,
            },
            'settings': {
                'my_yt_channels': True,
                'my_aw_accounts': True,
                'billing': True,
            },
        },
    }

    name = models.CharField(max_length=255, primary_key=True)
    permissions = JSONField(default=plan_preset['free'])

    @staticmethod
    def load_defaults():
        Plan.objects.all().delete()

        for key, value in Plan.plan_preset.items():
            Plan.objects.get_or_create(name=key, defaults=dict(permissions=value))

        # set admin plans
        plan = Plan.objects.get(name='full')
        users = UserProfile.objects.filter(is_staff=True)
        for user in users:
            user.plan = plan
            user.set_permissions_from_plan(plan)
            user.save()

        # set default plan for non-admin users
        plan = Plan.objects.get(name='free')
        users = UserProfile.objects.filter(plan__isnull=True)
        for user in users:
            user.plan = plan
            user.set_permissions_from_plan(plan)
            user.save()


class UserChannel(Timestampable):
    channel_id = models.CharField(max_length=30)
    user = models.ForeignKey(UserProfile, related_name="channels")

    class Meta:
        unique_together = ("channel_id", "user")
