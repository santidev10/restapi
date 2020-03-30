from django.core.management.base import BaseCommand
import logging
from audit_tool.models import AuditChannelMeta
logger = logging.getLogger(__name__)
from pid import PidFile

"""
requirements:
    sync large subscriber channels with viewiq for tracking
process:
    look at AuditChannelMeta objects with subscribers>=5000
    check and sync with viewiq if necessary.
"""

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('num_channels', type=int)

    def handle(self, *args, **options):
        self.num_channels = options.get('num_channels')
        if not self.thread_id:
            self.num_channels = 100000
        with PidFile(piddir='.', pidname='audit_sync_channels_with_viewiq.pid') as p:
            count = 0
            pending_channels = AuditChannelMeta.objects.filter(synced_with_viewiq__isnull=True, subscribers__gt=5000)
            total_pending = pending_channels.count()
            if total_pending == 0:
                logger.info("No channels to sync.")
                raise Exception("No channels to sync.")
            for channel_meta in pending_channels[:self.num_channels]:
                channel_id = channel_meta.channel.channel_id
                # CHECK HERE FOR ES DOCUMENT FOR THIS CHANNEL_ID
                # CREATE IF DOESN'T EXIST WITH MANUAL TRACKED=TRUE
                channel_meta.synced_with_viewiq = True
                channel_meta.save(update_fields=['synced_with_viewiq'])
                count+=1
            logger.info("Done {} channels".format(count))
            raise Exception("Done {} channels: {} total pending.".format(count, total_pending))
