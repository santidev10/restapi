from django.core.management.base import BaseCommand

from audit_tool.custom_audit import CustomAuditProvider


class Command(BaseCommand):
    help = 'Start Reaudit.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--title',
            help='Export title'
        )
        parser.add_argument(
            '--type',
            help='Video or channel audit'
        )
        parser.add_argument(
            '--export',
            help='Set file export result directory.'
        )
        parser.add_argument(
            '--file',
            help='Set file path of csv keywords to read from.'
        )
        parser.add_argument(
            '--whitelist',
            help='Set keywords file path.'
        )
        parser.add_argument(
            '--blacklist',
            help='More bad words'
        )

    def handle(self, *args, **kwargs):
        audit = CustomAuditProvider(*args, **kwargs)
        audit.run()