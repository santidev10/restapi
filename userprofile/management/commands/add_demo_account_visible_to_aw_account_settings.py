import logging

from django.contrib.auth import get_user_model
from django.core.management import BaseCommand

from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from userprofile.constants import UserSettingsKey

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Start add_demo_account_visible_to_aw_account_settings")
        count = 0
        for user in get_user_model().objects.all():
            if DEMO_ACCOUNT_ID not in user.aw_settings[UserSettingsKey.VISIBLE_ACCOUNTS]:
                count += 1
                user.aw_settings[UserSettingsKey.VISIBLE_ACCOUNTS].insert(0, DEMO_ACCOUNT_ID)
                user.save()
        logger.info("DONE add_demo_account_visible_to_aw_account_settings. %s users processed.", count)
