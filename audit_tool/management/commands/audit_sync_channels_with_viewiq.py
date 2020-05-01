from django.core.management.base import BaseCommand
from django.conf import settings
import logging
from channel.utils import track_channels
from audit_tool.models import AuditChannelMeta
logger = logging.getLogger(__name__)
from pid import PidFile
from audit_tool.management.commands.audit_fill_monetisation_data import Command as MonetizationCommand

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
        if not self.num_channels:
            self.num_channels = 100000
        try:
            sync_threshold = settings.AUDIT_SUBSCRIBER_SYNC_THRESHOLD
        except Exception as e:
            sync_threshold = 4000
        with PidFile(piddir='.', pidname='audit_sync_channels_with_viewiq.pid') as p:
            pending_channels = AuditChannelMeta.objects.filter(synced_with_viewiq__isnull=True, subscribers__gte=sync_threshold).order_by("-subscribers")
            total_pending = pending_channels.count()
            if total_pending == 0:
                logger.info("No channels to sync.")
                raise Exception("No channels to sync.")
            channel_ids = []
            monetized_channels = []
            meta_ids = []
            for channel_meta in pending_channels[:self.num_channels]:
                channel_id = channel_meta.channel.channel_id
                if channel_meta.monetised:
                    monetized_channels.append(channel_id)
                channel_ids.append(channel_id)
                meta_ids.append(channel_meta.id)
            try:
                track_channels(channel_ids)
            except Exception as e:
                print((str(e)))
                raise Exception(e)
            AuditChannelMeta.objects.filter(id__in=meta_ids).update(synced_with_viewiq=True)
            logger.info("Done {} channels, setting monetised channels".format(len(channel_ids)))
            if len(monetized_channels) > 0:
                monetise = MonetizationCommand()
                monetise.update_es_monetisation(monetized_channels)
            raise Exception("Done {} channels: {} total pending.".format(len(channel_ids), total_pending))
