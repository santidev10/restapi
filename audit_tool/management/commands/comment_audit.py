from django.core.management.base import BaseCommand
from audit_tool.comment_audit import CommentAudit
import logging
import os
import csv
import json

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    # def add_arguments(self, parser):
        # Positional arguments
        # parser.add_argument(
        #     '--file',
        #     help='Set csv file path'
        # )

    def handle(self, *args, **kwargs):
        comment_audit = CommentAudit()
        comment_audit.run()