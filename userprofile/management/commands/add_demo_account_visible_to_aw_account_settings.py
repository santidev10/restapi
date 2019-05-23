import logging

from django.contrib.auth import get_user_model
from django.core.management import BaseCommand

from userprofile.constants import UserSettingsKey


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Start")
        count = 0
        for user in get_user_model().objects.all():
            count += 1
            user_aw_settings = dict(**user.aw_settings)

            if UserSettingsKey.DEMO_ACCOUNT_VISIBLE not in user_aw_settings.keys():
                user.aw_settings[UserSettingsKey.DEMO_ACCOUNT_VISIBLE] = True
                user.save()
        logger.info("DONE {} users processed.".format(count))