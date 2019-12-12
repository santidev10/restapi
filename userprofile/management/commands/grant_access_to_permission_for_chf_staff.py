import logging
from userprofile.permissions import Permissions
from django.contrib.auth import get_user_model
from django.core.management import BaseCommand

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Gives the permission selected to all Channel Factory staff'

    def add_arguments(self, parser):
        parser.add_argument(
            'permission',
            help='Permission to be given',
            type=str
        )

    def handle(self, *args, **options):
        logger.info("Start")
        Permissions.sync_groups()
        count = 0
        permission_type = options["permission"]
        for user in get_user_model().objects.all():
            if user.email.lower().endswith("@channelfactory.com"):
                logger.info("- {}".format(user.email))
                count += 1
                user.add_custom_user_group(permission_type)
        logger.info("DONE {} users processed.".format(count))
