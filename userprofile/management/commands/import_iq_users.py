import csv
from dateutil.parser import parse as datetime_parse
import glob
import logging
import yaml

from django.conf import settings
from django.core.management import BaseCommand
from django.db import transaction

from userprofile.models import UserProfile

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    DEFAULT_AW_SETTINGS = {
        'dashboard_campaigns_segmented': False,
        'dashboard_ad_words_rates': False,
        'demo_account_visible': False,
        'dashboard_remarketing_tab_is_hidden': False,
        'dashboard_costs_are_hidden': False,
        'show_conversions': False,
        'visible_accounts': [],
        'hidden_campaign_types': {},
        'global_account_visibility': False,
    }

    CHF_AW_SETTINGS = {
        'dashboard_campaigns_segmented': True,
        'dashboard_ad_words_rates': True,
        'demo_account_visible': True,
        'dashboard_remarketing_tab_is_hidden': False,
        'dashboard_costs_are_hidden': False,
        'show_conversions': True,
        'visible_accounts': [],
        'hidden_campaign_types': {},
        'global_account_visibility': True,
    }

    def handle(self, *args, **options):
        with transaction.atomic():
            self.import_users()
            self.update_chf_users_permissions()

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
                                  "is_verified",
                                  "is_influencer",
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

                    user = UserProfile(**user_info)
                    user.aw_settings = sites_info.get(
                        name,
                        self.DEFAULT_AW_SETTINGS,
                    )
                    try:
                        UserProfile.objects.get(email=user.email)
                    except UserProfile.DoesNotExist:
                        logger.info("New account [{}]:  {}"\
                                    .format(name, user.email))
                        user.save()

    def update_chf_users_permissions(self):
        for user in UserProfile.objects.all():
            if user.email.lower().endswith('@channelfactory.com'):
                logger.info("CHF account found:" + user.email)
                user.is_verified = True
                user.is_influencer = True
                user.is_tos_signed = True
                user.is_comparison_tool_available = True
                user.is_subscribed_to_campaign_notifications = True
                user.aw_settings = self.CHF_AW_SETTINGS
                user.save()

            if not user.aw_settings:
                logger.info("Found account without aw_settings:" + user.email)
                user.aw_settings = self.DEFAULT_AW_SETTINGS
                user.save()
