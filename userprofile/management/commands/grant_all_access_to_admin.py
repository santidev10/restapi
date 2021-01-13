import logging

from django.contrib.auth import get_user_model
from django.core.management import BaseCommand

from userprofile.permissions import PermissionGroupNames
from userprofile.permissions import Permissions
from userprofile.models import PermissionItem

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Start")
        Permissions.sync_groups()
        PermissionItem.load_permissions()
        admin = get_user_model().objects.get(id=1)
        all_permissions = [perm for perm in dir(PermissionGroupNames) if not perm.startswith("__")]
        for perm in all_permissions:
            group_name = getattr(PermissionGroupNames, perm)
            admin.add_custom_user_group(group_name)
            logger.info("User %s has been granted access to permission %s.", admin.email, group_name)

