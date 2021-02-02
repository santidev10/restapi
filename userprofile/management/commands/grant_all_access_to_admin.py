import logging

from django.core.management import BaseCommand

from userprofile.models import PermissionItem

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Start")
        PermissionItem.load_permissions()
