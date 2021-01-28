import logging

from django.contrib.auth import get_user_model
from django.core.management import BaseCommand

from userprofile.constants import StaticPermissions
from userprofile.models import PermissionItem

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Gives the permission selected to all Channel Factory staff"

    def add_arguments(self, parser):
        parser.add_argument(
            "permission",
            help="Permission to be given",
            type=str
        )

    def handle(self, *args, **options):
        logger.info("Start")
        count = 0
        permission_type = options["permission"]
        if not getattr(StaticPermissions, permission_type, None):
            valid_perms = "\n".join(PermissionItem.all_perms())
            raise ValueError(f"Invalid permission: {permission_type}. Valid values: \n{valid_perms}")
        for user in get_user_model().objects.all():
            if user.email.lower().endswith("@channelfactory.com"):
                logger.info("- %s", user.email)
                count += 1
                user.perms.update({
                    getattr(StaticPermissions, permission_type): True
                })
                user.save()
        logger.info("DONE %s users processed.", count)
