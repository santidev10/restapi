"""
Userprofile models module
"""
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, \
    UserManager, Permission, Group
from django.contrib.contenttypes.models import ContentType
from django.contrib.postgres.fields import JSONField
from django.core import validators
from django.core.mail import send_mail
from django.db import models
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _

from userprofile.permissions import PermissionHandler
from utils.models import Timestampable


class UserSettingsKey:
    DASHBOARD_CAMPAIGNS_SEGMENTED = "dashboard_campaigns_segmented"
    DASHBOARD_AD_WORDS_RATES = "dashboard_ad_words_rates"
    DEMO_ACCOUNT_VISIBLE = "demo_account_visible"
    HIDE_REMARKETING = "dashboard_remarketing_tab_is_hidden"
    DASHBOARD_COSTS_ARE_HIDDEN = "dashboard_costs_are_hidden"
    SHOW_CONVERSIONS = "show_conversions"
    VISIBLE_ACCOUNTS = "visible_accounts"
    HIDDEN_CAMPAIGN_TYPES = "hidden_campaign_types"
    GLOBAL_ACCOUNT_VISIBILITY = "global_account_visibility"


DEFAULT_SETTINGS = {
    UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: False,
    UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
    UserSettingsKey.DEMO_ACCOUNT_VISIBLE: False,
    UserSettingsKey.HIDE_REMARKETING: False,
    UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
    UserSettingsKey.SHOW_CONVERSIONS: False,
    UserSettingsKey.VISIBLE_ACCOUNTS: [],
    UserSettingsKey.HIDDEN_CAMPAIGN_TYPES: {},
    UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: False,
}


class UserProfile(AbstractBaseUser, PermissionsMixin, PermissionHandler):
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
    address = models.CharField(max_length=255, null=True, blank=True)
    date_of_birth = models.DateField(null=True, blank=True)
    paypal_email = models.EmailField(null=True, blank=True)
    facebook_id = models.CharField(max_length=255, null=True, blank=True)
    is_password_generated = models.BooleanField(default=False)

    # professional info
    vertical = models.CharField(max_length=200, null=True, blank=True)
    worked_with = models.TextField(null=True, blank=True)
    price_range = models.TextField(null=True, blank=True)
    strong_beliefs = models.TextField(null=True, blank=True)

    # permission fields
    features_available = models.CharField(max_length=100, default="",
                                          blank=True)
    is_verified = models.BooleanField(default=False)
    is_influencer = models.BooleanField(default=False)
    is_tos_signed = models.BooleanField(default=True)
    is_comparison_tool_available = models.BooleanField(default=False)

    is_subscribed_to_campaign_notifications = models.BooleanField(default=True)

    aw_settings = JSONField(default=DEFAULT_SETTINGS)

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

    def get_aw_settings(self):
        settings = self.aw_settings
        for default_settings_key, default_settings_value in DEFAULT_SETTINGS.items():
            if default_settings_key not in settings:
                settings[default_settings_key] = default_settings_value
        return self.aw_settings

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

    @property
    def access(self):
        return self.groups.values('name')


class UserChannel(Timestampable):
    channel_id = models.CharField(max_length=30)
    user = models.ForeignKey(UserProfile, related_name="channels")

    class Meta:
        unique_together = ("channel_id", "user")
