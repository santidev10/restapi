from django.core.management.base import BaseCommand
from audit_tool.models import TopicAudit
from audit_tool.topic_audit import TopicAudit as TopicAuditor

import json
import logging
import os
import csv

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Execute a custom audit against existing channels and videos with a provided csv of audit words'

    def handle(self, *args, **kwargs):
        # this should run endelessly looking for new topic audits to start up
        topic_audits_to_run = TopicAudit.objects.filter(should_start=True, is_running=False)

        # Topics are topic objects derived from TopicAudit model
        for topic in topic_audits_to_run:
            # convert json field of keywords to list
            keywords = json.loads(topic.keywords)

            audit = TopicAuditor(
                topic=topic,
                channel_segment=topic.channel_segment,
                video_segment=topic.video_segment,
                keywords=keywords,
            )
            audit.run()

