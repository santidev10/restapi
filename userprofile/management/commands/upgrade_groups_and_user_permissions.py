"""
Command to update groups and users accesses
"""
import logging

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.management import BaseCommand

from userprofile.permissions import Permissions, PermissionGroupNames

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Start renaming groups")
        Group.objects.filter(name="Dashboard").update(name=PermissionGroupNames.MANAGED_SERVICE)
        Group.objects.filter(name="Segments").update(name=PermissionGroupNames.MEDIA_PLANNING)
        Group.objects.filter(name="Media buying").update(name=PermissionGroupNames.MEDIA_BUYING)
        logger.info("Done")
        logger.info("Start syncing groups")
        Permissions.sync_groups()
        logger.info("Done")
        logger.info("Updating users accesses")
        users = get_user_model().objects.all()
        media_buying_group = Group.objects.get(name=PermissionGroupNames.MEDIA_BUYING)
        groups_to_add = Group.objects.filter(
            name__in=[
                PermissionGroupNames.SELF_SERVICE,
                PermissionGroupNames.SELF_SERVICE_TRENDS,
                PermissionGroupNames.FORECASTING,
            ])
        for user in users:
            if media_buying_group in user.groups.all():
                for group in groups_to_add:
                    user.groups.add(group)
        logger.info("Done")
