from django.core.management.base import BaseCommand
import logging
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditProcessorCache
logger = logging.getLogger(__name__)
from datetime import timedelta
from django.utils import timezone
from pid import PidFile
import pytz

"""
process:
    Looks for processing audits, and calculates projected completion time
"""
# 7:47
class Command(BaseCommand):
    def handle(self, *args, **options):
        with PidFile(piddir='.', pidname='calculate_completion.pid') as p:
            audits = AuditProcessor.objects.filter(completed__isnull=True,started__lt=timezone.now()-timedelta(hours=1))
            for audit in audits:
                if not audit.cached_data or not audit.cached_data.get('total'):
                    continue
                avg_rate_sum = 0.0
                avg_rate_count = 0.0
                previous = None
                for db_history in AuditProcessorCache.objects.filter(
                        audit=audit, count__gt=0, created__gt=timezone.now() - timedelta(hours=1)).order_by("id"):
                    if previous:
                        rate = db_history.count - previous
                        avg_rate_count += 1.0
                        avg_rate_sum += rate
                    previous = db_history.count
                try:
                    avg_rate_per_minute = avg_rate_sum / avg_rate_count
                    num_minutes = (audit.cached_data.get('total') - audit.cached_data.get('count')) / avg_rate_per_minute
                    projected_completion = timezone.now() + timedelta(minutes=num_minutes)
                    audit.params['projected_completion'] = projected_completion.astimezone(pytz.timezone('America/Los_Angeles')).strftime("%m/%d %I:%M %p")
                    audit.save(update_fields=['params'])
                except Exception as e:
                    logger.info(str(e))
            raise Exception("Done {} projected times".audits.count())
