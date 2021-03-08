import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from pid.decorator import pidfile

from audit_tool.models import AuditMachine

logger = logging.getLogger(__name__)

"""
requirements:
    to report to the DB that a machine is alive
"""


class Command(BaseCommand):
    @pidfile(piddir="pids", pidname="audit_machine_check.pid")
    def handle(self, *args, **options):
        try:
            machine_number = settings.AUDIT_MACHINE_NUMBER
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            machine_number = 0
        a, _ = AuditMachine.objects.get_or_create(machine_number=machine_number)
        a.last_seen=timezone.now()
        a.save(update_fields=['last_seen'])
        logger.info("Done machine %s alive.", machine_number)
