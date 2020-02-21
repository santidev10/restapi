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
class Command(BaseCommand):
    def handle(self, *args, **options):
        with PidFile(piddir='.', pidname='calculate_completion.pid') as p:
            audits = AuditProcessor.objects.filter(completed__isnull=True,started__lt=timezone.now()-timedelta(hours=1))
            for audit in audits:
                if not audit.cached_data or not audit.cached_data.get('total'):
                    continue
                history = AuditProcessorCache.objects.filter(audit=audit, created__gt=timezone.now() - timedelta(hours=1))
                if history.count() < 15:
                    audit.params['projected_completion'] = None
                    audit.save(update_fields=['params'])
                    continue
                first = history.order_by("id")[0]
                last = history.order_by("-id")[0]
                count = last.count - first.count
                minutes = (last.created - first.created).total_seconds() / 60
                avg_rate_per_minute = count / minutes if minutes > 0 else 0
                if avg_rate_per_minute > 0:
                    num_minutes = (audit.cached_data.get('total') - last.count) / avg_rate_per_minute
                    projected_completion = timezone.now() + timedelta(minutes=num_minutes)
                    audit.params['projected_completion'] = projected_completion.astimezone(
                        pytz.timezone('America/Los_Angeles')).strftime("%m/%d %I:%M %p")
                    audit.params["avg_rate_per_minute"] = avg_rate_per_minute
                else:
                    audit.params["avg_rate_per_minute"] = None
                    audit.params['projected_completion'] = None
                audit.save(update_fields=['params'])
            raise Exception("Done {} projected times".format(audits.count()))
