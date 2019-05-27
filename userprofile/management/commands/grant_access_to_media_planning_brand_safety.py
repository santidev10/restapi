import logging

from django.contrib.auth import get_user_model
from django.core.management import BaseCommand

from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import Permissions

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Start grant_access_to_media_planning_brand_safety")
        Permissions.sync_groups()
        count = 0
        for user in get_user_model().objects.all():
            if PermissionGroupNames.MEDIA_PLANNING_BRAND_SAFETY not in user.get_user_groups():
                count += 1
                user.add_custom_user_group(PermissionGroupNames.MEDIA_PLANNING_BRAND_SAFETY)
        logger.info("DONE grant_access_to_media_planning_brand_safety. {} users processed.".format(count))
