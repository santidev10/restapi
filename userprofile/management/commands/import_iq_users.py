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
from userprofile.permissions import PermissionGroupNames

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    DEFAULT_AW_SETTINGS = get_default_settings()

    CHF_AW_SETTINGS = {
        UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True,
        UserSettingsKey.DEMO_ACCOUNT_VISIBLE: True,
        UserSettingsKey.HIDE_REMARKETING: False,
        UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: False,
        UserSettingsKey.SHOW_CONVERSIONS: True,
        UserSettingsKey.VISIBLE_ACCOUNTS: [],
        UserSettingsKey.HIDDEN_CAMPAIGN_TYPES: {},
        UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
    }

    def add_arguments(self, parser):
        parser.add_argument(
            '--update_visible_accounts_only',
            dest='update_visible_accounts_only',
            help='Update visible accounts_setting only',
            type=bool,
            default=False,
        )
    def handle(self, *args, **options):
        with transaction.atomic():
            if options['update_visible_accounts_only']:
                self.update_visible_accounts_only()
            else:
                self.import_users()
                self.update_users_permissions()

    def load_sites_info(self):
        mask = settings.BASE_DIR + "/userprofile/fixtures/sites/*.yml"
        result = {}
        for filename in glob.glob(mask):
            name = filename.split('/')[-1]
            if name.endswith(".yml"):
                name = name[:-4]
            with open(filename, 'r') as f:
                result[name] = yaml.load(f)
        return result

    def import_users(self):
        for name, user_info in self.get_users_info():
            user = UserProfile(**user_info)
            try:
                UserProfile.objects.get(email=user.email)
            except UserProfile.DoesNotExist:
                logger.info("New account [{}]:  {}" \
                            .format(name, user.email))
                user.save()

    def update_visible_accounts_only(self):
        for name, user_info in self.get_users_info():
            email = user_info["email"]
            try:
                user = UserProfile.objects.get(email=email)
            except UserProfile.DoesNotExist:
                logger.info("{} - does not exist\n".format(email))
                continue

            db_value = user.aw_settings["visible_accounts"]
            import_value = user_info["aw_settings"]["visible_accounts"]

            if db_value != import_value:
                logger.info("{}  different set of visible accounts\n"
                            "db    : {}\n"
                            "import: {}\n"
                            .format(
                                email,
                                ",".join(db_value),
                                ",".join(import_value),
                            )
                )
                user.aw_settings["visible_accounts"] = import_value
                user.save()

    def get_users_info(self):
        sites_info = self.load_sites_info()

        mask = settings.BASE_DIR + "/userprofile/fixtures/users/*.csv"
        for filename in glob.glob(mask):
            name = filename.split("/")[-1]
            if name.endswith(".csv"):
                name = name[:-4]
            with open(filename, "r") as f:
                reader = csv.DictReader(f)
                for user_info in reader:
                    del user_info["id"]

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
                    yield name, user_info

    def update_users_permissions(self):
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

            user.add_custom_user_group(PermissionGroupNames.HIGHLIGHTS)
