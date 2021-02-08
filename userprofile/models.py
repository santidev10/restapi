"""
Userprofile models module
"""
import binascii
import logging
import os
from uuid import uuid4

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AbstractBaseUser
from django.contrib.auth.models import PermissionsMixin
from django.contrib.auth.models import UserManager
from django.core import validators
from django.db import models
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _

from .constants import StaticPermissions
from administration.notifications import send_html_email
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.models import Opportunity
from userprofile.constants import DEFAULT_DOMAIN
from userprofile.constants import UserSettingsKey
from userprofile.permissions import PermissionGroupNames
from utils.models import Timestampable

logger = logging.getLogger(__name__)


def get_default_settings():
    return {
        UserSettingsKey.VISIBLE_ACCOUNTS: [DEMO_ACCOUNT_ID],
        UserSettingsKey.HIDDEN_CAMPAIGN_TYPES: {},
    }


def get_default_accesses(via_google=False):
    default_accesses_group_names = [
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
        case_insensitive_username_field = "{}__iexact".format(
            self.model.USERNAME_FIELD)
        return self.get(**{case_insensitive_username_field: username})


class LowercaseEmailField(models.EmailField):
    def get_prep_value(self, value):
        value = super(LowercaseEmailField, self).get_prep_value(value)
        return value.lower() if isinstance(value, str) else value


class UserProfile(AbstractBaseUser, PermissionsMixin):
    """
    An abstract base class implementing a fully featured User model with
    admin-compliant permissions.

    Username, password and email are required. Other fields are optional.
    """
    username = models.CharField(
        _("username"), max_length=30, blank=True, null=True,
        help_text=_("Required. 30 characters or fewer. Letters, digits and "
                    "@/./+/-/_ only."),
        validators=[
            validators.RegexValidator(
                r"^[\w.@+-]+$",
                _("Enter a valid username. "
                  "This value may contain only letters, numbers "
                  "and @/./+/-/_ characters."), "invalid"),
        ],
        error_messages={
            "unique": _("A user with that username already exists."),
        })
    first_name = models.CharField(_("first name"), max_length=30, blank=True)
    last_name = models.CharField(_("last name"), max_length=30, blank=True)
    is_staff = models.BooleanField(
        _("staff status"), default=False,
        help_text=_("Designates whether the user can log into this admin site."))
    is_active = models.BooleanField(
        _("active"), default=True,
        help_text=_("Designates whether this user should be treated as "
                    "active. Unselect this instead of deleting accounts."))
    date_joined = models.DateTimeField(_("date joined"), default=timezone.now)

    # extra fields and updated fields
    email = LowercaseEmailField(_("email address"), unique=True)
    company = models.CharField(max_length=255, null=True, blank=True)
    phone_number = models.CharField(max_length=15, null=True, blank=True)
    phone_number_verified = models.BooleanField(default=False)
    profile_image_url = models.URLField(null=True, blank=True)
    address = models.CharField(max_length=255, null=True, blank=True)
    date_of_birth = models.DateField(null=True, blank=True)
    paypal_email = models.EmailField(null=True, blank=True)
    facebook_id = models.CharField(max_length=255, null=True, blank=True)
    is_password_generated = models.BooleanField(default=False)
    google_account_id = models.CharField(null=True, blank=True, max_length=255)
    logo = models.CharField(null=True, blank=True, max_length=255)
    status = models.CharField(max_length=255, null=True, blank=True)
    perms = models.JSONField(default=dict)

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

    aw_settings = models.JSONField(default=get_default_settings)

    user_type = models.CharField(max_length=255, blank=True, null=True)
    annual_ad_spend = models.CharField(max_length=255, blank=True, null=True)
    synced_with_email_campaign = models.BooleanField(default=False, db_index=True)
    domain = models.ForeignKey("WhiteLabel", on_delete=models.SET_NULL, null=True)

    # GDPR Cookie Compliance
    has_accepted_GDPR = models.BooleanField(default=None, null=True)

    objects = UserProfileManager()

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = ["username"]

    def has_permission(self, perm):
        # if user is admin, they automatically get whatever permission
        if self.perms.get("admin") and self.perms.get("admin") is True:
            return True
        elif self.perms.get(perm) is not None:
            return self.perms[perm]
        else:
            # Attempt to return the default permission value being checked as permission was not set on user
            try:
                return PermissionItem.objects.get(permission=perm).default_value
            except Exception as e:
                raise Exception("invalid permission name")

    class Meta:
        """
        Meta params
        """
        verbose_name = _("user")
        verbose_name_plural = _("users")

    def __str__(self):
        """
        User instance string representation
        :return: str
        """
        return self.get_full_name() or self.email.split("@")[0]

    def get_full_name(self):
        """
        Returns the first_name plus the last_name, with a space in between.
        """
        full_name = "{} {}".format(self.first_name, self.last_name)
        return full_name.strip()

    def get_short_name(self):
        """
        Returns the short name for the user
        """
        return self.first_name

    def get_aw_settings(self):
        aw_settings = dict(**self.aw_settings)
        for default_settings_key, default_settings_value in get_default_settings().items():
            if default_settings_key not in aw_settings:
                aw_settings[default_settings_key] = default_settings_value
        return aw_settings

    def email_user_active(self, request):
        """
        Send email to user when admin makes it active
        """
        protocol = "http://"
        if request.is_secure():
            protocol = "https://"
        host = self.domain_name or f"www.{DEFAULT_DOMAIN}.com"
        host_address = f"{protocol}{host}"
        link = f"{host_address}/login"
        subject = "Access to ViewIQ"
        text_header = "Dear {} \n".format(self.get_full_name())
        text_content = "Congratulations! You now have access to ViewIQ!\n" \
                       " Click <a href='{link}'>here</a> to access your account." \
            .format(link=link)
        send_html_email(subject, self.email, text_header, text_content, host=host_address)

    @property
    def access(self):
        accesses = list(self.groups.values("name"))
        if self.is_staff:
            accesses.append({"name": "Admin"})
        return accesses

    @property
    def logo_url(self):
        logo_name = settings.USER_DEFAULT_LOGO if not self.logo else self.logo
        return settings.AMAZON_S3_LOGO_STORAGE_URL_FORMAT.format(logo_name)

    @property
    def domain_name(self):
        try:
            if self.domain.domain == DEFAULT_DOMAIN:
                domain_name = f"www.{DEFAULT_DOMAIN}.com"
            else:
                domain_name = f"{self.domain.domain}.{DEFAULT_DOMAIN}.com"
        except (WhiteLabel.DoesNotExist, AttributeError):
            domain_name = None
        return domain_name


class PermissionItem(models.Model):
    permission = models.CharField(unique=True, max_length=128)
    default_value = models.BooleanField(db_index=True, default=False)
    display = models.TextField(default="")

    STATIC_PERMISSIONS = [
    #   [FEATURE.PERMISSION_NAME,                       DEFAULT_VALUE, display]
        [StaticPermissions.ADMIN,                           False,  "Admin (the powers of Zeus)"],
        [StaticPermissions.ADS_ANALYZER,                    False,  "Ads Analyzer Read"],
        [StaticPermissions.ADS_ANALYZER__RECIPIENTS,        False,  "View all Ads Analyzer reports"],

        [StaticPermissions.AUDIT_QUEUE,                     False,  "Audit Queue Read"],
        [StaticPermissions.AUDIT_QUEUE__CREATE,             False,  "Audit Queue Create"],
        [StaticPermissions.AUDIT_QUEUE__SET_PRIORITY,       False,  "Audit Queue Set Audit Priority"],

        [StaticPermissions.BLOCKLIST_MANAGER,               False,  "Blocklist Manager Read"],
        [StaticPermissions.BLOCKLIST_MANAGER__CREATE,       False,  "Blocklist Manager Create"],
        [StaticPermissions.BLOCKLIST_MANAGER__DELETE,       False,  "Blocklist Manager Delete"],
        [StaticPermissions.BLOCKLIST_MANAGER__EXPORT,       False,  "Blocklist Manager Export"],

        [StaticPermissions.BSTL,                            False,  "Brand Safety Target List (BSTL) Read"],
        [StaticPermissions.BSTL__EXPORT,                    False,  "BSTL Export"],

        [StaticPermissions.BSTE,                            False,  "Brand Safety Tags Editor Read"],
        [StaticPermissions.BSTE__CREATE,                    False,  "Brand Safety Tags Editor Create"],
        [StaticPermissions.BSTE__DELETE,                    False,  "Brand Safety Tags Editor Delete"],
        [StaticPermissions.BSTE__EXPORT,                    False,  "Brand Safety Tags Editor Export"],

        [StaticPermissions.CHF_TRENDS,                      False,  "View CHF Trends Read"],

        [StaticPermissions.CTL,                             False,  "Custom Target Lists Read"],
        [StaticPermissions.CTL__CREATE,                     False,  "Create"],
        [StaticPermissions.CTL__DELETE,                     False,  "Delete"],
        [StaticPermissions.CTL__FEATURE_LIST,               False,  "Feature / Unfeature List"],
        [StaticPermissions.CTL__EXPORT_BASIC,               False,  "Export (basic)"],
        [StaticPermissions.CTL__EXPORT_ADMIN,               False,  "Export (all data)"],
        [StaticPermissions.CTL__SEE_ALL,                    False,  "See all Lists"],
        [StaticPermissions.CTL__VET,                        False,  "Vet Stuff"],
        [StaticPermissions.CTL__VET_ADMIN,                  False,  "Vet Admin"],
        [StaticPermissions.CTL__VET_EXPORT,                 False,  "Download Vetted only Export"],

        [StaticPermissions.DOMAIN_MANAGER,                  False,  "Domain Manager Read"],
        [StaticPermissions.DOMAIN_MANAGER__CREATE,          False,  "Domain Manager Create"],
        [StaticPermissions.DOMAIN_MANAGER__DELETE,          False,  "Domain Manager Delete"],

        [StaticPermissions.FORECAST_TOOL,                   False,  "Forecasting Tool"],

        [StaticPermissions.MANAGED_SERVICE,                             False,  "Managed Service Read"],
        [StaticPermissions.MANAGED_SERVICE__EXPORT,                     False,  "Managed Service Export"],
        [StaticPermissions.MANAGED_SERVICE__PERFORMANCE_GRAPH,          False,  "Managed Service Performance"],
        [StaticPermissions.MANAGED_SERVICE__DELIVERY,                   False,  "Managed Service Delivery"],
        [StaticPermissions.MANAGED_SERVICE__CAMPAIGNS_SEGMENTED,        False,  "Managed Service Campaigns Segmented"],
        [StaticPermissions.MANAGED_SERVICE__CONVERSIONS,                False,  "Managed Service Conversions"],
        [StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS,       False,  "Managed Service All Accounts Visible"],
        [StaticPermissions.MANAGED_SERVICE__REAL_GADS_COST,             False,  "Managed Service Real Google Ads Cost"],
        [StaticPermissions.MANAGED_SERVICE__GLOBAL_ACCOUNT_VISIBILITY,  False,  "Managed Service Global Accounts Visible"],
        [StaticPermissions.MANAGED_SERVICE__AUDIENCES,                  False,  "Managed Service Audiences Tab"],
        [StaticPermissions.MANAGED_SERVICE__SERVICE_COSTS,              False,  "Managed Service Service Costs"],
        [StaticPermissions.MANAGED_SERVICE__CHANNEL_VIDEO_TABS,         False,  "Managed Service Channel & Video Tabs"],

        [StaticPermissions.MEDIA_BUYING,                    False,  "Media Buying"],
        [StaticPermissions.PACING_REPORT,                   False,  "Pacing Report"],
        [StaticPermissions.PERFORMIQ,                       False,  "PerformIQ"],
        [StaticPermissions.PERFORMIQ__EXPORT,               False,  "Export"],
        [StaticPermissions.PRICING_TOOL,                    False,  "Pricing Tool"],

        [StaticPermissions.RESEARCH,                        True,   "Research Read"],
        [StaticPermissions.RESEARCH__AUTH,                  False,  "Auth channels/videos & audience data"],
        [StaticPermissions.RESEARCH__AGE_GENDER,            False,  "Age & gender data"],
        [StaticPermissions.RESEARCH__BRAND_SUITABILITY,     False,  "View brand suitability filters & badges"],
        [StaticPermissions.RESEARCH__BRAND_SUITABILITY_HIGH_RISK, False, "View brand suitability high risk filter"],

        [StaticPermissions.RESEARCH__CHANNEL_VIDEO_DATA,    False,  "Channel and Video detail data"],
        [StaticPermissions.RESEARCH__MONETIZATION,          False,  "Monetization data"],
        [StaticPermissions.RESEARCH__EXPORT,                False,  "Export"],
        [StaticPermissions.RESEARCH__TRANSCRIPTS,           False,  "Transcripts data"],
        [StaticPermissions.RESEARCH__VETTING,               False,  "Able to vet items"],
        [StaticPermissions.RESEARCH__VETTING_DATA,          False,  "View vetting data & filters"],

        [StaticPermissions.USER_ANALYTICS,                  False,  "User analytics"],
        [StaticPermissions.USER_MANAGEMENT,                 False,  "User management"],
    ]

    @staticmethod
    def load_permissions():
        for p in PermissionItem.STATIC_PERMISSIONS:
            defaults = dict(permission=p[0], default_value=p[1], display=p[2])
            PermissionItem.objects.update_or_create(permission=p[0], defaults=defaults)

    @classmethod
    def all_perms(cls):
        perm_names = [
            perm[0] for perm in cls.STATIC_PERMISSIONS if perm[0] not in StaticPermissions.DEPRECATED
        ]
        return perm_names


class UserChannel(Timestampable):
    channel_id = models.CharField(max_length=30)
    user = models.ForeignKey(UserProfile, related_name="channels", on_delete=models.CASCADE)

    class Meta:
        unique_together = ("channel_id", "user")


class UserDeviceToken(models.Model):
    user = models.ForeignKey(UserProfile, on_delete=models.CASCADE, related_name="tokens")
    key = models.CharField(max_length=45, unique=True)
    device_id = models.UUIDField(default=uuid4, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)

    @staticmethod
    def generate_key(is_temp=False):
        key = binascii.hexlify(os.urandom(20)).decode()
        if is_temp is True:
            key = f"temp_{key}"
        return key

    def update_key(self):
        self.key = self.generate_key()
        self.created_at = timezone.now()
        self.save()

    # pylint: disable=signature-differs
    def save(self, *args, **kwargs):
        if not self.key:
            self.key = self.generate_key()
        return super().save(*args, **kwargs)
    # pylint: enable=signature-differs


class WhiteLabel(models.Model):
    domain = models.CharField(max_length=255, unique=True)
    config = models.JSONField(default=dict)

    def __str__(self):
        return self.domain

    @staticmethod
    def get(domain):
        try:
            white_label = WhiteLabel.objects.get(domain=domain)
        except WhiteLabel.DoesNotExist:
            white_label, _ = WhiteLabel.objects.get_or_create(domain=DEFAULT_DOMAIN)
        return white_label

    @staticmethod
    def extract_sub_domain(host):
        try:
            domain = host.lower().split("viewiq")[0]
            sub_domain = domain.strip(".") or DEFAULT_DOMAIN
        except (IndexError, AttributeError):
            sub_domain = DEFAULT_DOMAIN
        return sub_domain


class Role(models.Model):
    name = models.CharField(max_length=100, db_index=True)
    permissions = models.ManyToManyField(PermissionItem, related_name="roles", db_index=True)


class UserRole(models.Model):
    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE, related_name="user_role")
    role = models.ForeignKey(Role, on_delete=models.SET_NULL, null=True)
