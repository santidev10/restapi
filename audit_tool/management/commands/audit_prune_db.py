from django.core.management.base import BaseCommand
import logging
from audit_tool.models import AuditProcessor
from django.utils import timezone
from datetime import timedelta
logger = logging.getLogger(__name__)
from pid import PidFile

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
        self.days = 90
        with PidFile(piddir='.', pidname='audit_prune_db.pid') as p:
            old_audits = AuditProcessor.objects.filter(source=0, completed__lt=timezone.now()-timedelta(days=self.days))
            count = 0
            for audit in old_audits:
                print("Deleting Audit {}".format(audit.id))
                audit.delete()
                count+=1
            print("Deleted {} Audits.".format(count))

