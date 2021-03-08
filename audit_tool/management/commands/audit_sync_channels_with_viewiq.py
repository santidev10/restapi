import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from pid import PidFile

from audit_tool.management.commands.audit_fill_monetisation_data import Command as MonetizationCommand
from audit_tool.models import AuditChannelMeta
from channel.utils import track_channels

logger = logging.getLogger(__name__)

"""
requirements:
    sync large subscriber channels with viewiq for tracking
process:
    look at AuditChannelMeta objects with subscribers>=5000
    check and sync with viewiq if necessary.
"""


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("num_channels", type=int)

    def handle(self, *args, **options):
        num_channels = options.get("num_channels")
        if not num_channels:
            num_channels = 100000
        try:
            sync_threshold = settings.AUDIT_SUBSCRIBER_SYNC_THRESHOLD
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            sync_threshold = 4000
        with PidFile(piddir="pids", pidname="audit_sync_channels_with_viewiq.pid"):
            pending_channels = AuditChannelMeta.objects \
                .filter(synced_with_viewiq__isnull=True,
                        subscribers__gte=sync_threshold) \
                .order_by("-subscribers")
            total_pending = pending_channels.count()
            if total_pending == 0:
                logger.info("No channels to sync.")
                raise Exception("No channels to sync.")
            channel_ids = []
            monetized_channels = []
            meta_ids = []
            for channel_meta in pending_channels[:num_channels]:
                channel_id = channel_meta.channel.channel_id
                if channel_meta.monetised:
                    monetized_channels.append(channel_id)
                channel_ids.append(channel_id)
                meta_ids.append(channel_meta.id)
            try:
                track_channels(channel_ids)
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                print((str(e)))
                raise Exception(e)
            AuditChannelMeta.objects.filter(id__in=meta_ids).update(synced_with_viewiq=True)
            logger.info("Done %s channels, setting monetised channels", len(channel_ids))
            if len(monetized_channels) > 0:
                monetise = MonetizationCommand()
                monetise.update_es_monetisation(monetized_channels)
            raise Exception("Done {} channels: {} total pending.".format(len(channel_ids), total_pending))
