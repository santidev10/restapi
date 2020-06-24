import logging

from django.contrib.auth import get_user_model
from django.core.management import BaseCommand

from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import Permissions

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Start")
        Permissions.sync_groups()
        count = 0
        for user in get_user_model().objects.all():
            if user.email.lower().endswith("@channelfactory.com"):
                logger.info("- %s", user.email)
                count += 1
                user.add_custom_user_group(PermissionGroupNames.MEDIA_PLANNING_AUDIT)
        logger.info("DONE %s users processed.", count)
