import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone
from pid import PidFile

from audit_tool.models import AuditProcessor

logger = logging.getLogger(__name__)

"""
requirements:
    remove all audit data that's older than 90 days, for DB sanity
process:
    look at AuditProcessor objects older than 90 days, and remove them.
    this should cascade delete AuditVideoProcessor, AuditChannelProcessor,
    and AuditExporter objects dynamically.
"""


class Command(BaseCommand):
    def handle(self, *args, **options):
        days = 180
        with PidFile(piddir=".", pidname="audit_prune_db.pid"):
            old_audits = AuditProcessor.objects.filter(source=0, completed__lt=timezone.now() - timedelta(
                days=days)).exclude(completed__isnull=True)
            count = 0
            for audit in old_audits:
                print("Deleting Audit {}".format(audit.id))
                audit.delete()
                count += 1
            print("Deleted {} Audits.".format(count))
