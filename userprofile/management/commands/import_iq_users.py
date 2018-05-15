import csv
import glob
import logging
import yaml

from django.conf import settings
from django.core.management import BaseCommand

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
        self.sites_info = self.load_sites_info()
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

    def load_users_info(self):
        mask = settings.BASE_DIR + "/userprofile/fixtures/users/*.csv"
        result = {}
        for filename in glob.glob(mask):
            name = filename.split('/')[-1]
            if name.endswith(".csv"):
                name = name[:-4]
            with open(filename, 'r') as f:
                reader = csv.DictReader(f)
                for user_info in reader:
                    del user_info['id']
                    user = UserProfile(**user_info)
                    user.aw_settings = self.sites_info.get(
                        name,
                        self.DEFAULT_AW_SETTINGS,
                    )
                    try:
                        UserProfile.objects.get(email=user.email)
                    except UserProfile.DoesNotExist:
                        user.save()

    def update_chf_users_permissions(self):
        for user in UserProfile.objects.all():
            if user.email.lower().endswith('@channelfactory.com')
                user.is_verified = True
                user.is_influencer = True
                user.is_tos_signed = True
                user.is_comparison_tool_available = True
                user.is_subscribed_to_campaign_notifications = True
                user.aw_settings = self.CHF_AW_SETTINGS
                user.save()

            if not user.aw_settings:
                user.aw_settings = self.DEFAULT_AW_SETTINGS