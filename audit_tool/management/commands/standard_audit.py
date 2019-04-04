from django.core.management.base import BaseCommand
from audit_tool.audit_providers.standard_audit import StandardAuditProvider
from audit_tool.models import APIScriptTracker
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    def handle(self, *args, **kwargs):
        api_tracker = APIScriptTracker.objects.get(name="StandardAudit")
        standard_audit = StandardAuditProvider(api_tracker=api_tracker)
        standard_audit.run()
