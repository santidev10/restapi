from django.core.management.base import BaseCommand
import logging
from django.db.models import Q
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditProcessorCache
from audit_tool.models import AuditVideoProcessor
logger = logging.getLogger(__name__)
from pid.decorator import pidfile
from django.utils import timezone
import datetime
from time import sleep

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
        loops = 0
        max_loops = 4
        # while loops < max_loops:
        audits = AuditProcessor.objects.filter(Q(completed__isnull=True) | Q(completed__gt=(timezone.now() - datetime.timedelta(hours=1))) ).order_by("-id")
        for audit in audits:
            count+=1
            self.do_audit_meta(audit)
        loops += 1
            # if loops < max_loops:
            #     sleep(15)
        AuditProcessorCache.objects.all().exclude(audit__in=audits).delete()
        logger.info("Done {} audits.".format(count))

    def do_audit_meta(self, audit):
        meta = {}
        audit_type = audit.params.get('audit_type_original')
        if not audit_type:
            audit_type = audit.audit_type
        if audit_type == 0:  # recommendation engine
            meta['total'] = audit.max_recommended
            count = AuditVideoProcessor.objects.filter(audit=audit, clean=True).count()
            meta['count'] = count
        elif audit_type == 1:  # process videos
            meta['total'] = AuditVideoProcessor.objects.filter(audit=audit).count()
            count = AuditVideoProcessor.objects.filter(audit=audit, processed__isnull=False).count()
            meta['count'] = count
        elif audit_type == 2:
            # if audit.params.get('do_videos'):
            meta['total'] = AuditChannelProcessor.objects.filter(audit=audit).count() + AuditVideoProcessor.objects.filter(audit=audit).count()
            count = AuditChannelProcessor.objects.filter(audit=audit, processed__isnull=False).count() + AuditVideoProcessor.objects.filter(audit=audit, processed__isnull=False).count()
            meta['count'] = count
            # else:
            #     meta['total'] = AuditChannelProcessor.objects.filter(audit=audit).count()
            #     meta['count'] = AuditChannelProcessor.objects.filter(audit=audit, processed__isnull=False).count()
        audit.cached_data = meta
        audit.save(update_fields=['cached_data'])
        if count == 0:
            AuditProcessorCache.objects.filter(audit=audit).delete()
        do_cache = False
        if not audit.completed:
            do_cache = True
        else:
            try:
                if count > AuditProcessorCache.objects.filter(audit=audit).order_by("-id")[0].count:
                    do_cache = True
            except Exception as e:
                pass
        if do_cache:
            AuditProcessorCache.objects.create(
                audit=audit,
                count=count
            )