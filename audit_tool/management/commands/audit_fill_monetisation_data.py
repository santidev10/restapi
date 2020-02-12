from django.core.management.base import BaseCommand
import logging
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditVideoProcessor
from es_components.managers import ChannelManager
from es_components.constants import Sections
from django.utils import timezone
from datetime import timedelta
logger = logging.getLogger(__name__)
from pid import PidFile

"""
requirements:
    to identify channels we ran in campaigns, and mark as monetised
process:
    look at AuditChannel objects that have been 'processed'
    and are 'campaign analysis' or 'campaign audits', and mark
    their corresponding channels as monetised in our DB.
"""

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('days', type=int)

    def handle(self, *args, **options):
        self.days = options.get('days')
        if not self.days:
            self.days = 3

        with PidFile(piddir='.', pidname='check_monetised_campaigns.pid') as p:
            # get video/channel meta audits
            self.audits = AuditProcessor.objects.filter(
                completed__gte=timezone.now() - timedelta(days=self.days)
            ).exclude(audit_type=0)
            self.manager = ChannelManager(sections=(Sections.MONETIZATION,),
                                          upsert_sections=(Sections.MONETIZATION,))
            self.process_audits()

    def process_audits(self):
        for audit in self.audits:
            if audit.name and not audit.params.get('done_monetised'):
                if 'campaign analysis' in audit.name.lower() or 'campaign audit' in audit.name.lower():
                    if audit.audit_type == 1:
                        self.mark_monetised_videos(audit)
                    else:
                        self.mark_monetised_channels(audit)
                    audit.params['done_monetised'] = True
                    audit.save(update_fields=['params'])

    def mark_monetised_videos(self, audit):
        videos = AuditVideoProcessor.objects.filter(audit=audit)
        for video in videos:
            try: # possible the channel object isn't set on this audit
                channel_meta = video.channel.auditchannelmeta
                channel_id = video.channel.channel_id
                channel = self.manager.get([channel_id])
                if not channel_meta.monetised:
                    channel_meta.monetised = True
                    channel_meta.save(update_fields=['monetised'])
                if channel:
                    channel.populate_monetization(is_monetizable=True)
            except Exception as e:
                pass

    def mark_monetised_channels(self, audit):
        channels = AuditChannelProcessor.objects.filter(audit=audit)
        for channel in channels:
            try: # possible the channel object isn't set on this audit
                channel_meta = channel.channel.auditchannelmeta
                channel_id = channel.channel.channel_id
                channel = self.manager.get([channel_id])
                if not channel_meta.monetised:
                    channel_meta.monetised = True
                    channel_meta.save(update_fields=['monetised'])
                if channel:
                    channel.populate_monetization(is_monetizable=True)
            except Exception as e:
                pass