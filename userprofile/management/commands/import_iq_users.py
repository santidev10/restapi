import csv
import glob
import logging

import yaml
from dateutil.parser import parse as datetime_parse
from django.conf import settings
from django.core.management import BaseCommand
from django.db import transaction

from userprofile.models import UserProfile
from userprofile.models import UserSettingsKey
from userprofile.models import get_default_settings
from userprofile.permissions import Permissions
from userprofile.permissions import PermissionGroupNames

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    IQ_FEATURES = (
        # dashboard
        ('0', 'dashboard'),
        ('1', 'adwords_accounts'),
        ('2', 'salesforce_accounts'),
        ('3', 'hourly_trends'),
        ('4', 'daily_trends'),
        ('5', 'pricing_tool'),
        ('6', 'keywords_tool'),
        ('7', 'pacing_reports'),
        ('8', 'opportunities'),
        ('9', 'operations'),
        ('a', 'salesforce_placements'),
        ('b', 'insights'),
        ('c', 'campaign_creation'),
        ('d', 'brands_page'),
        ('e', 'setup_health_check_tool'),
    )
    FEATURES_IDS = {n: uid for uid, n in IQ_FEATURES}

    files_suffix = ""

    DEFAULT_AW_SETTINGS = get_default_settings()

    CHF_AW_SETTINGS = {
        UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True,
        UserSettingsKey.DEMO_ACCOUNT_VISIBLE: True,
        UserSettingsKey.HIDE_REMARKETING: False,
        UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
        UserSettingsKey.SHOW_CONVERSIONS: True,
        UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        UserSettingsKey.HIDDEN_CAMPAIGN_TYPES: {},
        UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--fixtures_suffix",
            dest="fixtures_suffix",
            help="set fixtures suffix like '20180629'",
            type=str,
            default="",
        )

    def handle(self, *args, **options):
        self.files_suffix = options.get("fixtures_suffix")

        with transaction.atomic():
            Permissions.sync_groups()

            users_info = self.get_users_info()
            self.import_users(users_info)
            self.update_permissions(users_info)

    def load_sites_info(self):
        mask = settings.BASE_DIR + \
               "/userprofile/fixtures/sites/*" + self.files_suffix + ".yml"
        result = {}
        for filename in glob.glob(mask):
            name = filename.split('/')[-1]
            if name.endswith(".yml"):
                name = name[:-4]
            with open(filename, 'r') as f:
                result[name] = yaml.load(f)
        return result

    def import_users(self, users_info):
        for name, user_info in users_info:
            user = UserProfile(**user_info)
            try:
                UserProfile.objects.get(email=user.email)
            except UserProfile.DoesNotExist:
                logger.info("New account [{}]:  {}" \
                            .format(name, user.email))
                user.save()

    def get_users_info(self):
        result = []

        sites_info = self.load_sites_info()
        mask = settings.BASE_DIR + \
               "/userprofile/fixtures/users/*" + self.files_suffix + ".csv"
        for filename in glob.glob(mask):
            name = filename.split("/")[-1]
            if name.endswith(".csv"):
                name = name[:-4]
            with open(filename, "r") as f:
                reader = csv.DictReader(f)
                for user_info in reader:
                    del user_info["id"]
                    del user_info["is_verified"]
                    del user_info["is_influencer"]

                    for field in ["last_login", "date_joined", "date_of_birth"]:
                        if user_info[field]:
                            user_info[field] = datetime_parse(user_info[field])
                        else:
                            del user_info[field]

                    for field in ["is_superuser",
                                  "is_staff",
                                  "is_active",
                                  "is_tos_signed",
                                  "is_comparison_tool_available",
                                  "is_password_generated",
                                  "is_subscribed_to_campaign_notifications"]:
                        assert user_info[field] in ["t", "f", ""]
                        if user_info[field] == "t":
                            user_info[field] = True
                        elif user_info[field] == "f":
                            user_info[field] = False

                    for field in list(user_info.keys()):
                        if user_info[field] == "":
                            del user_info[field]

                    user_info["aw_settings"] = sites_info.get(
                        name,
                        self.DEFAULT_AW_SETTINGS,
                    )
                    result.append(tuple([name, user_info]))
                    logger.info("Loaded {}: {}".format(name, user_info["email"]))
        return result

    def update_users_permissions(self, users_info):
        for user in UserProfile.objects.all():
            if user.email.lower().endswith('@channelfactory.com'):
                logger.info("Updating CHF account:" + user.email)
                user.is_tos_signed = True
                user.is_comparison_tool_available = True
                user.is_subscribed_to_campaign_notifications = True
                user.aw_settings = self.CHF_AW_SETTINGS
                user.save()
                user.add_custom_user_group(PermissionGroupNames.TOOLS)
            else:
                logger.info("Updating account:" + user.email)

            if not user.aw_settings:
                logger.info("Found account without aw_settings:" + user.email)
                user.aw_settings = self.DEFAULT_AW_SETTINGS
                user.save()

            for group_name in settings.DEFAULT_PERMISSIONS_GROUP_NAMES:
                user.add_custom_user_group(group_name)
