from django.core.management.base import BaseCommand
from audit_tool.comment_audit import CommentAudit
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    def handle(self, *args, **kwargs):
        comment_audit = CommentAudit()
        comment_audit.run()