from django.core.management.base import BaseCommand
from audit_tool.topic_audit import TopicAudit as TopicAuditor
import logging

from pid.decorator import pidfile
from pid import PidFileAlreadyLockedError

logger = logging.getLogger('topic_audit')

class Command(BaseCommand):
    help = 'Start topic audit.'

    def handle(self, *args, **kwargs):
        try:
            self.run(*args, **kwargs)
        except PidFileAlreadyLockedError:
            pass

    @pidfile(piddir=".", pidname="topic_audit.pid")
    def run(self, *args, **kwargs):
        logger.info('Starting PID topic_audit.pid...')
        topic_audit = TopicAuditor()
        topic_audit.run()

