from django.db.models import F
from django.conf import settings
from django.core.management import BaseCommand

from es_components.constants import Sections
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager

from audit_tool.models import APIScriptTracker
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoVet
from utils.utils import chunks_generator


BATCH_SIZE = 1000

class Command(BaseCommand):
    def handle(self, *args, **options):
        channel_manager = ChannelManager(sections=[Sections.TASK_US_DATA])
        video_manager = VideoManager(sections=[Sections.TASK_US_DATA])
        update(AuditChannelVet, channel_manager, data_id="channel")
        update(AuditVideoVet, video_manager, data_id="video")


def update(vetting_model, es_manager, data_id="video"):
    vets = {}
    all_vets = vetting_model.objects.filter(processed__isnull=False).annotate(c_id=F(f"{data_id}__{data_id}_id"))
    for vet in all_vets:
        channel_id = vet.c_id
        try:
            curr = vets[channel_id]
            if curr.processed > curr.processed:
                vets[channel_id] = curr
        except KeyError:
            vets[channel_id] = vet
    for batch in chunks_generator(vets.values(), size=1000):
        batch = list(batch)
        docs = es_manager.get([vet.c_id for vet in batch], skip_none=True)

        with_vetted_data = {doc.main.id for doc in docs if doc.task_us_data}

        to_upsert = []
        for vet in batch:
            if vet.c_id not in with_vetted_data:
                continue
            doc = es_manager.model(vet.c_id)
            doc.populate_task_us_data(last_vetted_at=vet.processed)
            to_upsert.append(doc)
        es_manager.upsert(to_upsert)