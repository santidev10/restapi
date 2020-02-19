from django.core.management.base import BaseCommand
import logging
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditVideoProcessor
from es_components.managers import ChannelManager
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
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
        self.upsert_batch_size = options.get('upsert_batch_size')
        if not self.days:
            self.days = 3
        if not self.upsert_batch_size or self.upsert_batch_size > 10000:
            self.upsert_batch_size = 1000
        with PidFile(piddir='.', pidname='check_monetised_campaigns.pid') as p:
            self.channel_ids = set()
            # get video/channel meta audits
            self.audits = AuditProcessor.objects.filter(
                completed__gte=timezone.now() - timedelta(days=self.days)
            ).exclude(audit_type=0)
            self.manager = ChannelManager(sections=(Sections.MONETIZATION,),
                                          upsert_sections=(Sections.MONETIZATION,))
            self.process_audits()
            self.update_es_monetisation()

    def process_audits(self):
        count = 0
        audits = []
        for audit in self.audits:
            if audit.name and not audit.params.get('done_monetised'):
                if 'campaign analysis' in audit.name.lower() or 'campaign audit' in audit.name.lower():
                    if audit.audit_type == 1:
                        self.mark_monetised_videos(audit)
                    else:
                        self.mark_monetised_channels(audit)
                    audit.params['done_monetised'] = True
                    count += 1
                    audit.save(update_fields=['params'])
        print("Done {} audits synced".format(count))

    def mark_monetised_videos(self, audit):
        videos = AuditVideoProcessor.objects.filter(audit=audit)
        for video in videos:
            try: # possible the channel object isn't set on this audit
                channel_meta = video.channel.auditchannelmeta
                self.channel_ids.add(video.channel.channel_id)
                if not channel_meta.monetised:
                    channel_meta.monetised = True
                    channel_meta.save(update_fields=['monetised'])
            except Exception as e:
                pass

    def mark_monetised_channels(self, audit):
        channels = AuditChannelProcessor.objects.filter(audit=audit)
        for channel in channels:
            try: # possible the channel object isn't set on this audit
                channel_meta = channel.channel.auditchannelmeta
                self.channel_ids.add(channel.channel.channel_id)
                if not channel_meta.monetised:
                    channel_meta.monetised = True
                    channel_meta.save(update_fields=['monetised'])
            except Exception as e:
                pass

    def update_es_monetisation(self):
        channel_ids = list(self.channel_ids)
        print("Updating {} channels in ES.".format(len(channel_ids)))
        upsert_index = 0
        while upsert_index < len(channel_ids):
            try:
                not_monetized_query = QueryBuilder().build().must_not().term().field("monetization.is_monetizable")\
                    .value(True).get()
                channel_ids_query = QueryBuilder().build().must().terms().field("main.id")\
                    .value(channel_ids[upsert_index:upsert_index+self.upsert_batch_size]).get()
                self.manager.update_monetization(not_monetized_query+channel_ids_query, is_monetizable=True)
            except Exception as e:
                pass
            upsert_index += self.upsert_batch_size
