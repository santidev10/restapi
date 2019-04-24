from django.core.management.base import BaseCommand
import logging
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideoProcessor
logger = logging.getLogger(__name__)
from pid.decorator import pidfile
from django.utils import timezone
import datetime

"""
requirements:
    to speed up the API call to get the % completion of an audit
process:
    for each 'not yet completed' audit or recently completed audit
    grab the total # of videos to process, and the total number done and cache it.
"""

class Command(BaseCommand):

    @pidfile(piddir=".", pidname="audit_cache_meta.pid")
    def handle(self, *args, **options):
        count = 0
        audits = AuditProcessor.objects.all().order_by("-id")
        for audit in audits:
            if not audit.completed or audit.completed > timezone.now() - datetime.timedelta(hours=2):
                count+=1
                self.do_audit_meta(audit)
        logger.info("Done {} audits.".format(count))

    def do_audit_meta(self, audit):
        meta = {}
        if audit.audit_type == 0:  # recommendation engine
            meta['total'] = audit.max_recommended
            meta['count'] = AuditVideoProcessor.objects.filter(audit=audit).count()
        elif audit.audit_type == 1:  # process videos
            meta['total'] = AuditVideoProcessor.objects.filter(audit=audit).count()
            meta['count'] = AuditVideoProcessor.objects.filter(audit=audit, processed__isnull=False).count()
        audit.cached_data = meta
        audit.save(update_fields=['cached_data'])