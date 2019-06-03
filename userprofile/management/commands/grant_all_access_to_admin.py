import logging
from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import Permissions

from django.contrib.auth import get_user_model

from django.core.management import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Start")
        Permissions.sync_groups()
        admin = get_user_model().objects.get(id=1)
        all_permissions = [perm for perm in dir(PermissionGroupNames) if not perm.startswith('__')]
        for perm in all_permissions:
            group_name = getattr(PermissionGroupNames, perm)
            admin.add_custom_user_group(group_name)
            logger.info("User {} has been granted access to permission {}.".format(admin.email, group_name))
