"""
Userprofile models module
"""
import logging

from django.conf import settings
from django.contrib.auth.models import AbstractBaseUser
from django.contrib.auth.models import PermissionsMixin
from django.contrib.auth.models import UserManager
from django.contrib.postgres.fields import JSONField
from django.core import validators
from django.core.mail import send_mail
from django.db import models
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _

from administration.notifications import send_html_email
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.models.ad_words.connection import AWConnectionToUserRelation
from userprofile.constants import UserSettingsKey
from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import PermissionHandler
from utils.models import Timestampable

logger = logging.getLogger(__name__)


def get_default_settings():
    return {
        UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: False,
        UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
        UserSettingsKey.HIDE_REMARKETING: False,
        UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
        UserSettingsKey.SHOW_CONVERSIONS: False,
        UserSettingsKey.VISIBLE_ACCOUNTS: [DEMO_ACCOUNT_ID],
        UserSettingsKey.VISIBLE_ALL_ACCOUNTS: False,
        UserSettingsKey.HIDDEN_CAMPAIGN_TYPES: {},
        UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: False,
    }


def get_default_accesses(via_google=False):
    default_accesses_group_names = [
        PermissionGroupNames.HIGHLIGHTS,
        PermissionGroupNames.RESEARCH,
        PermissionGroupNames.MEDIA_PLANNING,
        PermissionGroupNames.MEDIA_PLANNING_BRAND_SAFETY,
        PermissionGroupNames.FORECASTING,
        PermissionGroupNames.BRAND_SAFETY_SCORING,
    ]
    if not via_google:
        default_accesses_group_names.append(PermissionGroupNames.MANAGED_SERVICE)
    return default_accesses_group_names


class UserProfileManager(UserManager):
    def get_by_natural_key(self, username):
        case_insensitive_username_field = '{}__iexact'.format(
            self.model.USERNAME_FIELD)
        return self.get(**{case_insensitive_username_field: username})


class LowercaseEmailField(models.EmailField):
    def get_prep_value(self, value):
        value = super(LowercaseEmailField, self).get_prep_value(value)
        return value.lower() if isinstance(value, str) else value


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
    email = LowercaseEmailField(_('email address'), unique=True)
    company = models.CharField(max_length=255, null=True, blank=True)
    phone_number = models.CharField(max_length=15, null=True, blank=True)
    profile_image_url = models.URLField(null=True, blank=True)
    address = models.CharField(max_length=255, null=True, blank=True)
    date_of_birth = models.DateField(null=True, blank=True)
    paypal_email = models.EmailField(null=True, blank=True)
    facebook_id = models.CharField(max_length=255, null=True, blank=True)
    is_password_generated = models.BooleanField(default=False)
    google_account_id = models.CharField(null=True, blank=True, max_length=255)
    logo = models.CharField(null=True, blank=True, max_length=255)
    status = models.CharField(max_length=255, null=True, blank=True)

    # professional info
    vertical = models.CharField(max_length=200, null=True, blank=True)
    worked_with = models.TextField(null=True, blank=True)
    price_range = models.TextField(null=True, blank=True)
    strong_beliefs = models.TextField(null=True, blank=True)

    # permission fields
    features_available = models.CharField(max_length=100, default="",
                                          blank=True)
    is_tos_signed = models.BooleanField(default=True)
    is_comparison_tool_available = models.BooleanField(default=False)

    is_subscribed_to_campaign_notifications = models.BooleanField(default=True)

    aw_settings = JSONField(default=get_default_settings)

    user_type = models.CharField(max_length=255, blank=True, null=True)
    annual_ad_spend = models.CharField(max_length=255, blank=True, null=True)
    synced_with_email_campaign = models.BooleanField(default=False, db_index=True)

    # GDPR Cookie Compliance
    has_accepted_GDPR = models.NullBooleanField(default=None)

    objects = UserProfileManager()

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
        settings = dict(**self.aw_settings)
        for default_settings_key, default_settings_value in get_default_settings().items():
            if default_settings_key not in settings:
                settings[default_settings_key] = default_settings_value
        return settings

    def email_user(self, subject, message, from_email=settings.SENDER_EMAIL_ADDRESS, **kwargs):
        """
        Sends an email to this User.
        """
        send_mail(subject, message, from_email, [self.email], **kwargs)

    def email_user_active(self, request):
        """
        Send email to user when admin makes it active
        """
        host = request.get_host()
        protocol = "http://"
        if request.is_secure():
            protocol = "https://"
        link = "{}{}/login".format(protocol, host)
        subject = "Access to ViewIQ"
        text_header = "Dear {} \n".format(self.get_full_name())
        text_content = "Congratulations! You now have access to ViewIQ!\n" \
                       " Click <a href='{link}'>here</a> to access your account." \
            .format(link=link)
        send_html_email(subject, self.email, text_header, text_content)

    @property
    def token(self):
        """
        User auth token
        """
        return self.auth_token.key

    @property
    def access(self):
        accesses = list(self.groups.values('name'))
        if self.is_staff:
            accesses.append({"name": "Admin"})
        return accesses

    @property
    def logo_url(self):
        logo_name = settings.USER_DEFAULT_LOGO if not self.logo else self.logo
        return settings.AMAZON_S3_LOGO_STORAGE_URL_FORMAT.format(logo_name)


class UserChannel(Timestampable):
    channel_id = models.CharField(max_length=30)
    user = models.ForeignKey(UserProfile, related_name="channels", on_delete=models.CASCADE)

    class Meta:
        unique_together = ("channel_id", "user")
